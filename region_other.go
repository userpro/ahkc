//go:build !(linux && (amd64 || arm64) && !race)

package ahkc

func mmapRegion(int, bool) ([]byte, error) { return nil, errNoMmap }

func munmapRegion([]byte) error { return nil }
