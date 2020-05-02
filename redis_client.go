package rmq

import "time"

// TODO: consistent return value naming
type RedisClient interface {
	// simple keys
	Set(key string, value string, expiration time.Duration) error
	Del(key string) (affected int64, err error)    // default affected: 0
	TTL(key string) (ttl time.Duration, err error) // default ttl: 0

	// lists
	LPush(key string, value ...string) (total int64, err error)
	LLen(key string) (affected int64, err error)
	LRem(key string, count int64, value string) (affected int64, err error)
	LTrim(key string, start, stop int64) error
	RPopLPush(source, destination string) (value string, ok bool, err error)

	// sets
	SAdd(key, value string) error
	SMembers(key string) (members []string, err error)  // default members: []string{}
	SRem(key, value string) (affected int64, err error) // default affected: 0

	// special
	FlushDb()
}
