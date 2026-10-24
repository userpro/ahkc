#!/usr/bin/env python3
"""生成验证报告：environment / checks / summary。

设计原则：这些报告都能由 bench/reproduce.sh 重新生成，因此不进版本库，
只在文档里引用它们产出的数字。任何字段都必须来自本机实测或源码哈希，
不允许手工编辑。

源码指纹算法（SourceSHA256）：
    取根包生产文件（*.go 且非 *_test.go）+ go.mod，按文件名排序，
    依次写入 "文件名" + NUL + 该文件内容的 SHA256 原始 32 字节 + NUL，
    对整体再取 SHA256。测试、example/、cmd/、docs/ 不计入。
"""

import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CGROUP_FILES = [
    "/sys/fs/cgroup/cpu.max",
    "/sys/fs/cgroup/cpuset.cpus.effective",
    "/sys/fs/cgroup/memory.max",
    "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
    "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
    "/sys/fs/cgroup/memory/memory.limit_in_bytes",
]


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def source_manifest():
    names = sorted(
        n for n in os.listdir(ROOT) if n.endswith(".go") and not n.endswith("_test.go")
    )
    names.append("go.mod")
    files = {n: sha256_file(os.path.join(ROOT, n)) for n in names}
    h = hashlib.sha256()
    for n in names:
        h.update(n.encode())
        h.update(b"\0")
        h.update(bytes.fromhex(files[n]))
        h.update(b"\0")
    return h.hexdigest(), files


def run(cmd):
    """执行命令，返回 (stdout, ok)，失败不抛异常。"""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return p.stdout.strip(), p.returncode == 0
    except Exception:
        return "", False


def lscpu():
    out, ok = run(["lscpu"])
    if not ok:
        return {"Platform": platform.platform()}
    d = {}
    for line in out.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            if k.strip() and v.strip():
                d[k.strip()] = v.strip()
    return d


def cgroup_limits():
    d = {}
    for p in CGROUP_FILES:
        try:
            with open(p) as f:
                d[p] = f.read().strip()
        except OSError:
            continue
    return d


def go_info():
    ver, _ = run(["go", "version"])
    env, _ = run(["go", "env", "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS"])
    lines = env.splitlines()
    keys = ["GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS"]
    return ver, {k: (lines[i] if i < len(lines) else "") for i, k in enumerate(keys)}


def build_env_doc():
    gover, goenv = go_info()
    affinity = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else []
    digest, files = source_manifest()
    cpus = len(affinity) or (os.cpu_count() or 0)
    return {
        "RecordedUTC": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "Host": platform.node(),
        "Kernel": platform.release(),
        "GoVersion": gover,
        "GoEnv": goenv,
        "Hardware": lscpu(),
        "CgroupLimits": cgroup_limits(),
        "Affinity": affinity,
        "RunnableCPUs": cpus,
        "SourceSHA256": digest,
        "SourceSHA256Method": (
            "sorted root production *.go (excluding *_test.go) + go.mod; "
            "SHA256 over (name, NUL, raw file digest, NUL)*"
        ),
        "SourceFiles": files,
        "ServiceObjective": {
            "MinSuccessfulQPS": 1000000,
            "MaxP99NS": 3000000,
            "FinalErrors": 0,
            "LoaderDelayNS": 500000,
        },
        "Measurement": (
            "Open-loop millisecond batches; latency from scheduled arrival through "
            "value access and borrow/handle release, including backlog, bounded retries "
            "and GC, with a real loader callback."
        ),
        "Limits": (
            "Numbers depend on this host's CPU topology, scheduling, GC pacing and "
            "co-resident load. Fixed seeds reproduce the client request sequence, not "
            "maphash placement, sampling or scheduling. Not evidence for 32 physical "
            "cores or cross-NUMA scaling."
        ),
    }


def build_checks_doc(tsv_path):
    rows = []
    with open(tsv_path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            name, command, code, seconds = line.split("\t")
            rows.append(
                {
                    "Name": name,
                    "Command": command,
                    "ExitCode": int(code),
                    "Seconds": round(float(seconds), 3),
                    "Result": "PASS" if code == "0" else "FAIL",
                }
            )
    digest, _ = source_manifest()
    gover, _ = go_info()
    return {
        "RecordedUTC": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "GoVersion": gover,
        "SourceSHA256": digest,
        "AllPassed": all(r["Result"] == "PASS" for r in rows),
        "Checks": rows,
        "Notes": (
            "Logs for each check are written next to this file as checks-<name>.log. "
            "386 entries only cover cross-compilation; 32-bit execution is not claimed."
        ),
    }


RUN_FIELDS = [
    "Scenario",
    "ReadMode",
    "HotPercent",
    "Keys",
    "Shards",
    "Seed",
    "OfferedQPS",
    "DurationSeconds",
    "SuccessfulQPS",
    "P50NS",
    "P95NS",
    "P99NS",
    "MaxNS",
    "Requests",
    "Successes",
    "Errors",
    "Busy",
    "Overloaded",
    "Hits",
    "Misses",
    "SourceLoads",
    "SourceMerged",
    "MaxManaged",
]


def build_summary_doc(paths):
    runs = []
    for p in paths:
        if not os.path.exists(p):
            continue
        with open(p) as f:
            d = json.load(f)
        item = {k: d.get(k) for k in RUN_FIELDS if k in d}
        item["File"] = os.path.basename(p)
        objective = {"MinSuccessfulQPS": 1000000, "MaxP99NS": 3000000}
        item["MeetsObjective"] = bool(
            (d.get("SuccessfulQPS") or 0) >= objective["MinSuccessfulQPS"]
            and (d.get("P99NS") or 0) <= objective["MaxP99NS"]
            and d.get("Errors", 1) == 0
        )
        item["ExpectedFailure"] = "4096" in os.path.basename(p) or "no-retry" in p
        runs.append(item)
    digest, files = source_manifest()
    return {
        "RecordedUTC": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "SourceSHA256": digest,
        "SourceFiles": len(files),
        "Runs": runs,
        "AllPassed": all(
            r["MeetsObjective"] or r["ExpectedFailure"] for r in runs
        ),
        "Notes": (
            "Every row is generated by bench/reproduce.sh from the source fingerprint "
            "above. ExpectedFailure marks the deliberately failing samples kept as "
            "negative evidence."
        ),
    }


def main(argv):
    if len(argv) < 3:
        sys.exit(__doc__)
    kind, out_path = argv[1], argv[2]
    if kind == "env":
        doc = build_env_doc()
    elif kind == "checks":
        doc = build_checks_doc(argv[3])
    elif kind == "summary":
        doc = build_summary_doc(argv[3:])
    else:
        sys.exit("unknown report kind: " + kind)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(doc, f, indent=2, sort_keys=False)
        f.write("\n")
    print("wrote", out_path)


if __name__ == "__main__":
    main(sys.argv)
