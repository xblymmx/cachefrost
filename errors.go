package cachefrost

import "errors"

var (
	ErrorKeyNotFound = errors.New("key not found in cache")
	ErrKeyNotFoundOrLoadable = errors.New("key not found and could not be loaded into cache")
)
