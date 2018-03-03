package cachefrost

import "sync"

var (
	cache = make(map[string]*CacheTable)
	mutex sync.RWMutex
)

func Cache(tablename string) *CacheTable {
	mutex.RLock()
	t, ok := cache[tablename]
	mutex.RUnlock()

	if !ok {
		mutex.Lock()
		// double check
		t, ok := cache[tablename]
		if !ok {
			t = &CacheTable{
				name:  tablename,
				items: make(map[interface{}]*CacheItem),
			}
			cache[tablename] = t
		}
		mutex.Unlock()
	}

	return t
}
