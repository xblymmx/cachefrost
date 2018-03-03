package cachefrost

import (
	"sync"
	"time"
)

type CacheItem struct {
	sync.RWMutex

	key interface{}
	data interface{}

	lifeSpan time.Duration

	createdOn time.Time // created time
	accessedOn time.Time // last accessed time

	accessCount int64

	aboutToExpire func(key interface{})

}


func NewCacheItem(key interface{}, lifeSpan time.Duration, data interface{}) *CacheItem {
	now := time.Now()
	return &CacheItem{
		key: key,
		data: data,
		lifeSpan: lifeSpan,
		createdOn: now,
		accessedOn: now,
		accessCount: 0,
		aboutToExpire: nil,
	}
}


func (item *CacheItem) KeepAlive() {
	item.Lock()
	defer item.Unlock()
	item.accessedOn = time.Now()
	item.accessCount++
}

func (item *CacheItem) LifeSpan() time.Duration {
	return item.lifeSpan
}

func (item *CacheItem) AccessedOn() time.Time {
	return item.accessedOn
}

func (item *CacheItem) CreatedOn() time.Time {
	return item.createdOn
}

func (item *CacheItem) Key() interface{} {
	return item.key
}

func (item *CacheItem) Data() interface{} {
	return item.data
}


func (item *CacheItem) SetAboutToExpire(f func(interface{})) {
	item.Lock()
	defer item.Unlock()
	item.aboutToExpire = f
}

