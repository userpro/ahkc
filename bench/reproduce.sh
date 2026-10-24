#!/usr/bin/env bash
# 一键复现 docs/VALIDATION*.md 中的全部验证数据。
#
# 所有产物写入 bench/results/（已被 .gitignore 忽略，不入库）；
# 文档只保留最近一次运行的数字，原始报告随时可以用本脚本重建。
#
# 用法：
#   ./bench/reproduce.sh                              # 完整复现，约 5–8 分钟
#   DURATION=2s BENCHTIME=100ms ./bench/reproduce.sh  # 冒烟，数字不可与文档对照
#
# 产物：
#   environment.json        本机硬件、cgroup、Go 环境、源码指纹（SourceSHA256）
#   checks.json             build / vet / gofmt / test / race / 386 交叉编译
#   microbench.txt          根包微基准（含 IndexHitProtected 基线）
#   microbench-public.txt   bench 包公开 API 微基准
#   allocations-feature.txt 功能矩阵基准
#   allocations-read.txt    读取路径 B/op 与 allocs/op
#   allocations-init.txt    初始化分配（普通 / 仅统计）
#   observe-switch.txt      观测开关并行开销
#   hotkeys.txt             HotKeys(20) 快照成本
#   hot-load.json           纯借用命中
#   mixed-load.json         95/3/2 混合借用
#   mixed-repeat-load.json  同上，第二随机种子
#   skew-load.json          99.9% 集中 / 64 key
#   owned-load.json         95/3/2 混合拥有型
#   skew-4096keys-load.json 失败样本：默认 4096 key 长尾冷却被判 miss
#   policy-replay.json / calibration.json / search-oracle.json  模型报告
#   summary.json           上述压测场景汇总 + 是否达标
#
# 证据边界：报告绑定 environment.json 的 SourceSHA256。换机器/换代码后必须重跑，
# 比较时关注验收阈值（成功 QPS ≥100 万、P99 ≤3ms、最终错误 = 0），不要要求
# 逐字节一致——调度、GC 节奏与同机干扰都会改变数字。
set -uo pipefail
cd "$(dirname "$0")/.."

DURATION="${DURATION:-20s}"
BENCHTIME="${BENCHTIME:-1s}"
OUT="${OUT:-bench/results}"
STATUS=0

mkdir -p "$OUT"
echo "==> 产物目录：$OUT（DURATION=$DURATION, BENCHTIME=$BENCHTIME）"

# ---------------------------------------------------------------- [1/6] 环境
echo "==> [1/6] 环境与源码指纹 -> $OUT/environment.json"
python3 bench/report.py env "$OUT/environment.json" || STATUS=1
SOURCE_SHA=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['SourceSHA256'])" "$OUT/environment.json" 2>/dev/null || echo unknown)
echo "    SourceSHA256=$SOURCE_SHA"

# ---------------------------------------------------------------- [2/6] 校验
echo "==> [2/6] 功能检查与竞态检测 -> $OUT/checks.json"
CHECKS_TSV="$OUT/checks.tsv"
: >"$CHECKS_TSV"

record() {
  local name="$1"
  shift
  local start end code secs
  start=$(date +%s.%N)
  "$@" >"$OUT/checks-$name.log" 2>&1
  code=$?
  end=$(date +%s.%N)
  secs=$(awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", b-a}')
  printf '%s\t%s\t%d\t%s\n' "$name" "$*" "$code" "$secs" >>"$CHECKS_TSV"
  if [ "$code" -eq 0 ]; then
    echo "    PASS $name (${secs}s)"
  else
    echo "    FAIL $name (${secs}s, 见 $OUT/checks-$name.log)"
    STATUS=1
  fi
}

record build go build ./...
record vet go vet ./...
record gofmt sh -c 'test -z "$(gofmt -l .)"'
record test go test -count=1 -timeout=180s ./...
record race go test -race -count=1 -timeout=600s ./...
record cross386-build env GOARCH=386 go build ./...
record cross386-vet env GOARCH=386 go vet ./...

python3 bench/report.py checks "$OUT/checks.json" "$CHECKS_TSV" || STATUS=1

# ------------------------------------------------------------ [3/6] 微基准
echo "==> [3/6] 微基准 -> $OUT/microbench.txt, $OUT/microbench-public.txt"
go test -run '^$' -bench 'Benchmark(BorrowHit|IndexHitProtected|GetHit|GetHitParallel|GetMissParallel)$' \
  -benchmem -benchtime="$BENCHTIME" -count=2 2>&1 | tee "$OUT/microbench.txt"
[ "${PIPESTATUS[0]}" -eq 0 ] || STATUS=1

go test -run '^$' -bench . -benchmem -benchtime="$BENCHTIME" ./bench 2>&1 | tee "$OUT/microbench-public.txt"
[ "${PIPESTATUS[0]}" -eq 0 ] || STATUS=1

echo "    分配 / 功能矩阵 / 观测开关 / HotKeys（数据用于文档的分配与开销表）"
go test -run '^$' -bench '^BenchmarkFeature' -benchmem -benchtime=10000x -count=5 \
  2>&1 | tee "$OUT/allocations-feature.txt"
[ "${PIPESTATUS[0]}" -eq 0 ] || STATUS=1

go test -run '^$' -bench '^BenchmarkReadAllocations$' -benchmem -benchtime=1000000x -count=5 \
  2>&1 | tee "$OUT/allocations-read.txt"
[ "${PIPESTATUS[0]}" -eq 0 ] || STATUS=1

go test -run '^$' -bench '^BenchmarkInitialize$' -benchmem -benchtime=5x -count=5 \
  2>&1 | tee "$OUT/allocations-init.txt"
[ "${PIPESTATUS[0]}" -eq 0 ] || STATUS=1

go test -run '^$' -bench '^BenchmarkObserveGetHitParallel$' -benchmem -benchtime=200ms -count=3 \
  2>&1 | tee "$OUT/observe-switch.txt"
[ "${PIPESTATUS[0]}" -eq 0 ] || STATUS=1

go test -run '^$' -bench '^BenchmarkHotKeys20$' -benchmem -count=3 \
  2>&1 | tee "$OUT/hotkeys.txt"
[ "${PIPESTATUS[0]}" -eq 0 ] || STATUS=1

# ------------------------------------------------------------ [4/6] 压测
# $1=产物名 $2=expected|required，其余透传给 cacheload
run_load() {
  local name="$1" expectation="$2"
  shift 2
  echo "==> [4/6] 压测 $name"
  go run ./cmd/cacheload "$@" -duration "$DURATION" -output "$OUT/$name.json" 2>&1 | tee "$OUT/load-$name.log"
  local code=${PIPESTATUS[0]}
  if [ "$code" -eq 0 ]; then
    echo "    PASS $name"
    return 0
  fi
  if [ "$expectation" = "expected" ]; then
    echo "    FAIL $name（预期：失败样本，长尾冷却被判 miss）"
    return 0
  fi
  echo "    FAIL $name（见 $OUT/load-$name.log）"
  STATUS=1
  return 0
}

run_load hot-load required -scenario hot
run_load mixed-load required -scenario mixed
run_load mixed-repeat-load required -scenario mixed -seed 20261002
run_load skew-load required -scenario mixed -hot-percent 99.9 -keys 64
run_load owned-load required -scenario mixed -read-mode owned
run_load skew-4096keys-load expected -scenario mixed -hot-percent 99.9

# ------------------------------------------------------------ [5/6] 模型
echo "==> [5/6] 模型报告（策略回放 / 合并率校准 / 有限搜索 oracle）"
AHKC_REPLAY_REPORT="$OUT/policy-replay.json" \
AHKC_CALIBRATION_REPORT="$OUT/calibration.json" \
AHKC_ORACLE_REPORT="$OUT/search-oracle.json" \
go test -run 'Test(SyntheticPolicyReplay|BoundedSearchAgainstFullResidentOracle|NoCacheSingleflightCalibration)$' -count=1 \
  >"$OUT/model.log" 2>&1 || { echo "    FAIL 模型报告（见 $OUT/model.log）"; STATUS=1; }
for f in policy-replay calibration search-oracle; do
  [ -f "$OUT/$f.json" ] && echo "    PASS $f.json"
done

# ------------------------------------------------------------ [6/6] 汇总
echo "==> [6/6] 汇总 -> $OUT/summary.json"
python3 bench/report.py summary "$OUT/summary.json" \
  "$OUT/hot-load.json" \
  "$OUT/mixed-load.json" \
  "$OUT/mixed-repeat-load.json" \
  "$OUT/skew-load.json" \
  "$OUT/owned-load.json" \
  "$OUT/skew-4096keys-load.json" || STATUS=1

python3 - "$OUT/summary.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print("    %-24s %12s %10s %8s %s" % ("scenario", "QPS", "P99(ms)", "errors", "verdict"))
for r in d["Runs"]:
    verdict = "PASS" if r["MeetsObjective"] else ("expected-fail" if r["ExpectedFailure"] else "FAIL")
    print("    %-24s %12.0f %10.3f %8d %s" % (
        r["File"].replace("-load.json", ""), r["SuccessfulQPS"], r["P99NS"] / 1e6, r["Errors"], verdict))
print("    all passed:", d["AllPassed"])
PY

if [ "$STATUS" -eq 0 ]; then
  echo "==> 完成：全部检查与场景达标，产物在 $OUT/"
else
  echo "==> 完成：存在未达标项，见上方 FAIL 行与对应日志（产物在 $OUT/）"
fi
exit "$STATUS"
