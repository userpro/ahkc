//go:build linux && (amd64 || arm64) && !race

package ahkc

import "syscall"

// madvHugepage 是 MADV_HUGEPAGE（syscall 包未导出该常量）。
const madvHugepage = 14

func mmapRegion(size int, hugePages bool) ([]byte, error) {
	b, err := syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE|syscall.MAP_NORESERVE)
	if err != nil {
		return nil, err
	}
	if hugePages {
		_ = syscall.Madvise(b, madvHugepage) // 失败只是少一个优化
	}
	return b, nil
}

func munmapRegion(b []byte) error { return syscall.Munmap(b) }
