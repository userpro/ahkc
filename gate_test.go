package ahkc

import (
	"testing"
)

// 门卫默认尺寸按容量近似：clamp(每分片条数额度/2, 16384, 2^24)，向上取 8 的倍数；
// 显式设置同样对齐到整条缓存行。
func TestGateCellsDefaults(t *testing.T) {
	cfg := validConfig()
	cfg.applyDefaults()
	if cfg.Heat.GateCells != 16384 {
		t.Fatalf("default GateCells = %d, want 16384", cfg.Heat.GateCells)
	}
	cfg = validConfig()
	cfg.MaxEntries, cfg.Shards = 100_000_000, 64
	cfg.applyDefaults()
	if want := 100_000_000 / 64 / 2; cfg.Heat.GateCells != (want+7)/8*8 {
		t.Fatalf("GateCells = %d, want %d", cfg.Heat.GateCells, (want+7)/8*8)
	}
	cfg = validConfig()
	cfg.Heat.GateCells = 100
	c, err := NewBytes[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeManual(t, c, cfg.Runtime.Executor.(*ManualExecutor))
	if got := c.c.cfg.Heat.GateCells; got != 104 || len(c.c.shards[0].gate.cells) != 104 {
		t.Fatalf("GateCells = %d, gate cells = %d, want 104", got, len(c.c.shards[0].gate.cells))
	}
}
