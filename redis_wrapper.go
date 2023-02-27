package rmq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisWrapper struct {
	rawClient redis.Cmdable
}

func (wrapper RedisWrapper) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.Set(ctx, key, value, expiration).Err()
}

func (wrapper RedisWrapper) Del(ctx context.Context, key string) (affected int64, err error) {
	return wrapper.rawClient.Del(ctx, key).Result()
}

func (wrapper RedisWrapper) TTL(ctx context.Context, key string) (ttl time.Duration, err error) {
	return wrapper.rawClient.TTL(ctx, key).Result()
}

func (wrapper RedisWrapper) LPush(ctx context.Context, key string, value ...string) (total int64, err error) {
	return wrapper.rawClient.LPush(ctx, key, value).Result()
}

func (wrapper RedisWrapper) LLen(ctx context.Context, key string) (affected int64, err error) {
	return wrapper.rawClient.LLen(ctx, key).Result()
}

func (wrapper RedisWrapper) LRem(ctx context.Context, key string, count int64, value string) (affected int64, err error) {
	return wrapper.rawClient.LRem(ctx, key, int64(count), value).Result()
}

func (wrapper RedisWrapper) LTrim(ctx context.Context, key string, start, stop int64) error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.LTrim(ctx, key, int64(start), int64(stop)).Err()
}

func (wrapper RedisWrapper) RPop(ctx context.Context, key string) (value string, err error) {
	return wrapper.rawClient.RPop(ctx, key).Result()
}

func (wrapper RedisWrapper) RPopLPush(ctx context.Context, source, destination string) (value string, err error) {
	value, err = wrapper.rawClient.RPopLPush(ctx, source, destination).Result()
	// println("RPopLPush", source, destination, value, err)
	switch err {
	case nil:
		return value, nil
	case redis.Nil:
		return value, ErrorNotFound
	default:
		return value, err
	}
}

func (wrapper RedisWrapper) SAdd(ctx context.Context, key, value string) (total int64, err error) {
	return wrapper.rawClient.SAdd(ctx, key, value).Result()
}

func (wrapper RedisWrapper) SMembers(ctx context.Context, key string) (members []string, err error) {
	return wrapper.rawClient.SMembers(ctx, key).Result()
}

func (wrapper RedisWrapper) SRem(ctx context.Context, key, value string) (affected int64, err error) {
	return wrapper.rawClient.SRem(ctx, key, value).Result()
}

func (wrapper RedisWrapper) FlushDb(ctx context.Context) error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.FlushDB(ctx).Err()
}
