package rmq

import "time"

// TODO: consistent return value naming
type RedisClient interface {
	// simple keys
	Set(key string, value string, expiration time.Duration) (ok bool, err error) // TODO: remove bool
	Del(key string) (affected int, ok bool, err error)                           // default affected: 0 // TODO: remove bool
	TTL(key string) (ttl time.Duration, ok bool, err error)                      // default ttl: 0 // TODO: remove bool

	// lists
	LPush(key string, value ...string) (bool, error)                             // TODO: remove bool
	LLen(key string) (affected int, ok bool, err error)                          // TODO: remove bool
	LRem(key string, count int, value string) (affected int, ok bool, err error) // TODO: remove bool
	LTrim(key string, start, stop int) (ok bool, err error)                      // TODO: remove bool
	RPopLPush(source, destination string) (value string, ok bool, err error)

	// sets
	SAdd(key, value string) (ok bool, err error)               // TODO: remove bool
	SMembers(key string) (members []string, err error)         // default members: []string{}
	SRem(key, value string) (affected int, ok bool, err error) // default affected: 0 // TODO: remove bool

	// special
	FlushDb()
}
