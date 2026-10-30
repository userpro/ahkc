package ahkc

import "hash/maphash"

// mix64 是 splitmix64 的终态混合函数，用于行哈希与采样判断。
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// hashKey 使用每实例随机种子计算完整 key 的哈希（0 堆分配）。
func hashKey(seed maphash.Seed, key string) uint64 {
	return maphash.String(seed, key)
}

// hashKey includes the namespace in the full key identity without allocating.
// Hash collisions still require an exact namespace and string comparison.
func (n *namespace[V]) hashKey(key string) uint64 {
	return mix64(hashKey(n.c.seed, key) ^ uint64(n.id)*rowSeedStep)
}
