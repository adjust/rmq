package rmq

import (
	"context"
	"time"
)

type RedisClient interface {
	// simple keys
	Set(ctx context.Context, key string, value string, expiration time.Duration) error
	Del(ctx context.Context, key string) (affected int64, err error)
	TTL(ctx context.Context, key string) (ttl time.Duration, err error)

	// lists
	LPush(ctx context.Context, key string, value ...string) (total int64, err error)
	LLen(ctx context.Context, key string) (affected int64, err error)
	LRem(ctx context.Context, key string, count int64, value string) (affected int64, err error)
	LTrim(ctx context.Context, key string, start, stop int64) error
	RPopLPush(ctx context.Context, source, destination string) (value string, err error)
	RPop(ctx context.Context, key string) (value string, err error)

	// sets
	SAdd(ctx context.Context, key, value string) (total int64, err error)
	SMembers(ctx context.Context, key string) (members []string, err error)
	SRem(ctx context.Context, key, value string) (affected int64, err error)

	// special
	FlushDb(ctx context.Context) error
}
