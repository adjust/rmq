package rmq

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

var unusedContext = context.TODO()

type RedisClusterWrapper struct {
	rawClient *redis.ClusterClient
}

func (wrapper RedisClusterWrapper) Set(key string, value string, expiration time.Duration) error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.Set(unusedContext, key, value, expiration).Err()
}

func (wrapper RedisClusterWrapper) Del(key string) (affected int64, err error) {
	return wrapper.rawClient.Del(unusedContext, key).Result()
}

func (wrapper RedisClusterWrapper) TTL(key string) (ttl time.Duration, err error) {
	return wrapper.rawClient.TTL(unusedContext, key).Result()
}

func (wrapper RedisClusterWrapper) LPush(key string, value ...string) (total int64, err error) {
	return wrapper.rawClient.LPush(unusedContext, key, value).Result()
}

func (wrapper RedisClusterWrapper) LLen(key string) (affected int64, err error) {
	return wrapper.rawClient.LLen(unusedContext, key).Result()
}

func (wrapper RedisClusterWrapper) LRem(key string, count int64, value string) (affected int64, err error) {
	return wrapper.rawClient.LRem(unusedContext, key, int64(count), value).Result()
}

func (wrapper RedisClusterWrapper) LTrim(key string, start, stop int64) error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.LTrim(unusedContext, key, int64(start), int64(stop)).Err()
}

func (wrapper RedisClusterWrapper) RPop(key string) (value string, err error) {
	return wrapper.rawClient.RPop(unusedContext, key).Result()
}

func (wrapper RedisClusterWrapper) RPopLPush(source, destination string) (value string, err error) {
	value, err = wrapper.rawClient.RPopLPush(unusedContext, source, destination).Result()
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

func (wrapper RedisClusterWrapper) SAdd(key, value string) (total int64, err error) {
	return wrapper.rawClient.SAdd(unusedContext, key, value).Result()
}

func (wrapper RedisClusterWrapper) SMembers(key string) (members []string, err error) {
	return wrapper.rawClient.SMembers(unusedContext, key).Result()
}

func (wrapper RedisClusterWrapper) SRem(key, value string) (affected int64, err error) {
	return wrapper.rawClient.SRem(unusedContext, key, value).Result()
}

func (wrapper RedisClusterWrapper) FlushDb() error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.FlushDB(unusedContext).Err()
}
