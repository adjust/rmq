package rmq

import "time"

type RedisClient interface {
	// simple keys
	Set(key string, value string, expiration time.Duration) (ok bool, err error)
	Del(key string) (affected int, ok bool, err error)      // default affected: 0
	TTL(key string) (ttl time.Duration, ok bool, err error) // default ttl: 0 // TODO: remove ok?

	// lists
	LPush(key string, value ...string) (bool, error)
	LLen(key string) (affected int, ok bool, err error)
	LRem(key string, count int, value string) (affected int, ok bool, err error)
	LTrim(key string, start, stop int) (ok bool, err error)
	RPopLPush(source, destination string) (value string, ok bool, err error)

	// sets
	SAdd(key, value string) (ok bool, err error)
	SMembers(key string) (members []string, err error)         // default members: []string{}
	SRem(key, value string) (affected int, ok bool, err error) // default affected: 0

	// special
	FlushDb()
}
