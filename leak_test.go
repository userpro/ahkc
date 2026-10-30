package ahkc

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// mappedBytes 返回进程全部映射的总字节数（linux）。
func mappedBytes(t *testing.T) int64 {
	t.Helper()
	b, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Skip("no /proc/self/maps")
	}
	var total int64
	for _, l := range strings.Split(string(b), "\n") {
		var lo, hi int64
		if _, err := fmt.Sscanf(l, "%x-%x", &lo, &hi); err == nil {
			total += hi - lo
		}
	}
	return total
}

// 非法配置必须在预留内存区之前被拒绝：newCache 里 validate 先于分片创建，
// 命名空间在预留之后才注册，所以靠这个顺序保证失败的构造不泄漏映射。
// 先用一次成功的构造作正向对照，证明这种度量确实能测到预留。
func TestInvalidConfigReservesNoMemory(t *testing.T) {
	if b, err := mmapRegion(4096, false); err != nil {
		t.Skip("mmap backend unavailable")
	} else {
		_ = munmapRegion(b)
	}
	cfg := validConfig()
	cfg.Shards, cfg.MaxEntries, cfg.MaxBytes = 2, 1000, 8<<20
	before := mappedBytes(t)
	c, err := NewBytes[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	if grew := mappedBytes(t) - before; grew < 8<<20 {
		t.Fatalf("control: a valid construction reserved only %d KiB", grew>>10)
	}
	closeManual(t, c, cfg.Runtime.Executor.(*ManualExecutor))

	cfg.Namespaces = map[string]NamespaceConfig{"bad": {TTL: -time.Second}}
	before = mappedBytes(t)
	for i := 0; i < 10; i++ {
		if c, err := NewBytes[string](cfg); err == nil {
			_ = c.Close(nil)
			t.Fatal("expected a construction error")
		}
	}
	if grew := mappedBytes(t) - before; grew > 4<<20 {
		t.Fatalf("invalid configs reserved %d KiB of mappings", grew>>10)
	}
}
