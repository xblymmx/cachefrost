package cachefrost

import (
	"sync"
	"time"
	"log"
	"sort"
)

type CacheTable struct {
	sync.RWMutex

	name  string
	items map[interface{}]*CacheItem

	cleanupTimer    *time.Timer
	cleanupInterval time.Duration

	logger *log.Logger

	// callback when load an un-existing key
	loadData func(key interface{}, data ...interface{}) *CacheItem

	// added callback
	addedItem func(item *CacheItem)

	// delete callback
	aboutToDeleteItem func(item *CacheItem)
}

// return table's item count
func (table *CacheTable) Count() int {
	table.Lock()
	defer table.Unlock()
	return len(table.items)
}

func (table *CacheTable) Foreach(trans func(interface{}, *CacheItem)) {
	table.Lock()
	defer table.Unlock()

	for key, item := range table.items {
		trans(key, item)
	}
}

func (table *CacheTable) SetDataLoader(loader func(interface{}, ...interface{}) *CacheItem) {
	table.Lock()
	defer table.Unlock()
	table.loadData = loader
}

func (table *CacheTable) SetAddedItemCallback(f func(*CacheItem)) {
	table.Lock()
	defer table.Unlock()
	table.addedItem = f
}

func (table *CacheTable) SetAboutToDeleteItemCallback(f func(*CacheItem)) {
	table.Lock()
	defer table.Unlock()
	table.aboutToDeleteItem = f
}

func (table *CacheTable) SetLogger(logger *log.Logger) {
	table.Lock()
	defer table.Unlock()
	table.logger = logger
}

// Expiration check loop, triggered by a self-adjusting timer.
func (table *CacheTable) expirationCheck() {
	table.Lock()
	defer table.Unlock()

	if table.cleanupTimer != nil {
		table.cleanupTimer.Stop()
	}
	if table.cleanupInterval > 0 {
		table.log("Expiration check after", table.cleanupInterval, "for table", table.name)
	} else {
		table.log("Expiration check set for table", table.name)
	}

	now := time.Now()
	smallestDuration := time.Duration(0)
	for key, item := range table.items {
		item.RLock()
		accessedOn := item.accessedOn
		lifeSpan := item.lifeSpan
		item.RUnlock()

		if lifeSpan == 0 { // infinite lifespan
			continue
		}

		if now.Sub(accessedOn) > lifeSpan { // excessed lifespan
			table.deleteInternal(key)
		} else {
			// Find the closest time to all items end-of-lifespan.
			if smallestDuration == 0 || lifeSpan-now.Sub(accessedOn) < smallestDuration {
				smallestDuration = lifeSpan - now.Sub(accessedOn)
			}
		}
	}

	table.cleanupInterval = smallestDuration
	if smallestDuration > 0 {
		table.cleanupTimer = time.AfterFunc(smallestDuration, func() {
			go table.expirationCheck()
		})
	}

	// table unlock
}

func (table *CacheTable) addInternal(item *CacheItem) {
	// Careful: do not run this method unless the table-mutex is locked!
	// It will unlock it for the caller before running the callbacks and checks
	table.log("Add item with key", item.key, "and lifespan of", item.lifeSpan, "to table", table.name)
	table.items[item.key] = item

	// Cache values so we don't keep blocking the mutex.
	interval := table.cleanupInterval
	addedItem := table.addedItem
	table.Unlock() // table has been locked

	if addedItem != nil {
		addedItem(item)
	}

	// If we haven't set up any expiration check timer or found a more imminent item.
	if item.lifeSpan > 0 && (interval == 0 || item.lifeSpan < interval) {
		table.expirationCheck()
	}
}

func (table *CacheTable) Add(key interface{}, lifeSpan time.Duration, data interface{}) *CacheItem {
	item := NewCacheItem(key, lifeSpan, data)
	table.Lock()
	table.addInternal(item)
	return item
}

func (table *CacheTable) deleteInternal(key interface{}) (*CacheItem, error) {
	item, ok := table.items[key]
	if !ok {
		return nil, ErrorKeyNotFound
	}

	aboutToDeleteItem := table.aboutToDeleteItem
	table.Unlock() // table has been locked in table.Delete(key)
	if aboutToDeleteItem != nil {
		aboutToDeleteItem(item)
	}

	item.RLock()
	defer item.RUnlock()
	if item.aboutToExpire != nil {
		item.aboutToExpire(key)
	}

	table.Lock()
	table.log("Deleting item with key", key, "created on", item.createdOn, "and hit", item.accessCount, "times from table", table.name)
	delete(table.items, key)

	return item, nil
}

func (table *CacheTable) log(v ...interface{}) {
	if table.logger == nil {
		return
	}

	table.logger.Println(v)
}

func (table *CacheTable) Exists(key interface{}) bool {
	table.RLock()
	defer table.RUnlock()
	_, ok := table.items[key]
	return ok
}

func (table *CacheTable) NotFoundAdd(key interface{}, lifeSpan time.Duration, data interface{}) bool {
	table.Lock()

	if _, ok := table.items[key]; ok {
		table.Unlock()
		return false
	}

	item := NewCacheItem(key, lifeSpan, data)
	table.addInternal(item)
	return true
}

// Value returns an item from the cache and marks it to be kept alive. You can
// pass additional arguments to your DataLoader callback function.
func (table *CacheTable) Value(key interface{}, args ...interface{}) (*CacheItem, error) {
	table.RLock()
	item, ok := table.items[key]
	loadData := table.loadData
	table.RUnlock()

	if ok {
		item.KeepAlive()
		return item, nil
	}

	// Item doesn't exist in cache. Try and fetch it with a args-loader.
	if loadData != nil {
		item := loadData(key, args...)
		if item != nil {
			table.Add(key, item.lifeSpan, item.data)
			return item, nil
		}

		return nil, ErrKeyNotFoundOrLoadable
	}

	return nil, ErrorKeyNotFound
}

func (table *CacheTable) Flush() {
	table.Lock()
	defer table.Unlock()

	table.log("flushing table", table.name)

	table.items = make(map[interface{}]*CacheItem)
	table.cleanupInterval = 0
	if table.cleanupTimer != nil {
		table.cleanupTimer.Stop()
	}
}

type CacheItemPair struct {
	Key         interface{}
	AccessCount int64
}

type CacheItemPairList []CacheItemPair

func (list CacheItemPairList) Swap(i, j int)      { list[i], list[j] = list[j], list[i] }
func (list CacheItemPairList) Len() int           { return len(list) }
func (list CacheItemPairList) Less(i, j int) bool { return list[i].AccessCount > list[j].AccessCount }

func (table *CacheTable) MostAccessed(count int64) []*CacheItem {
	table.RLock()
	defer table.RUnlock()

	list := make(CacheItemPairList, len(table.items))
	i := 0
	for k, v := range table.items {
		list[i] = CacheItemPair{Key: k, AccessCount: v.accessCount}
	}

	sort.Sort(list)

	var result []*CacheItem
	c := int64(0)

	for _, p := range list {
		if c >= count {
			break
		}

		item, ok := table.items[p.Key]
		if ok {
			result = append(result, item)
		}
		c++
	}

	return result
}

func (table *CacheTable) Delete(key interface{}) (*CacheItem, error) {
	table.Lock()
	defer table.Unlock()
	return table.deleteInternal(key)
}

