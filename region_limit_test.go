//go:build linux && (amd64 || arm64) && !race

package ahkc

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// 虚拟内存受限时 New 必须返回 ErrInvalidConfig，而不是回退到同样大小的堆分配后 fatal error 崩溃。
// 在子进程里设 RLIMIT_AS：限制对测试进程自身生效太晚（运行时已经映射了地址空间）。
func TestNewReturnsErrorWhenAddressSpaceIsLimited(t *testing.T) {
	if os.Getenv("AHKC_RLIMIT_CHILD") == "1" {
		lim := syscall.Rlimit{Cur: 1 << 30, Max: 1 << 30}
		if err := syscall.Setrlimit(syscall.RLIMIT_AS, &lim); err != nil {
			t.Fatalf("setrlimit: %v", err)
		}
		// 区约 14 GiB，远超 1 GiB 地址空间上限；热点表走同一条路径，各验证一次。
		for _, hot := range []HotConfig{{}, {RequestSlots: 1 << 20, RequestSampleP: 1, Windows: 2}} {
			cfg := Config{
				Shards: 4, MaxEntries: 50_000_000, MaxBytes: 12 << 30, Hot: hot,
				Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Hour, Tin: 1}},
			}
			if hot.RequestSlots > 0 { // 区本身放得下，只有键池超限
				cfg.MaxEntries, cfg.MaxBytes = 1000, 1<<20
				cfg.MaxKeyBytes = 4096
			}
			c, err := NewBytes[string](cfg)
			if err == nil {
				c.Close(nil)
				t.Fatalf("New succeeded under a 1 GiB address-space limit: %+v", hot)
			}
			if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "address space") {
				t.Fatalf("want ErrInvalidConfig naming the address space, got %v", err)
			}
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestNewReturnsErrorWhenAddressSpaceIsLimited$", "-test.v")
	cmd.Env = append(os.Environ(), "AHKC_RLIMIT_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child failed (a crash here is the bug): %v\n%s", err, tail(out, 1500))
	}
}

func tail(b []byte, n int) string {
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}
