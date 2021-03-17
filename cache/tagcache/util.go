package tagcache

func legalKey(key string) bool {
	if len(key) > 250 || len(key) == 0 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}
