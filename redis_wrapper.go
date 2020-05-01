package rmq

import (
	"time"

	"github.com/go-redis/redis/v7"
)

type RedisWrapper struct {
	rawClient *redis.Client
}

func (wrapper RedisWrapper) Set(key string, value string, expiration time.Duration) error {
	return wrapper.rawClient.Set(key, value, expiration).Err()
}

func (wrapper RedisWrapper) Del(key string) (affected int64, err error) {
	return wrapper.rawClient.Del(key).Result()
}

func (wrapper RedisWrapper) TTL(key string) (ttl time.Duration, err error) {
	return wrapper.rawClient.TTL(key).Result()
}

func (wrapper RedisWrapper) LPush(key string, value ...string) error {
	return wrapper.rawClient.LPush(key, value).Err()
}

func (wrapper RedisWrapper) LLen(key string) (affected int64, err error) {
	return wrapper.rawClient.LLen(key).Result()
}

func (wrapper RedisWrapper) LRem(key string, count int64, value string) (affected int64, err error) {
	return wrapper.rawClient.LRem(key, int64(count), value).Result()
}

func (wrapper RedisWrapper) LTrim(key string, start, stop int64) error {
	return wrapper.rawClient.LTrim(key, int64(start), int64(stop)).Err()
}

// TODO: comment on bool return value and how/why we transform redis.Nil to nil error
func (wrapper RedisWrapper) RPopLPush(source, destination string) (value string, ok bool, err error) {
	value, err = wrapper.rawClient.RPopLPush(source, destination).Result()
	switch err {
	case nil:
		return value, true, nil
	case redis.Nil:
		return value, false, nil
	default:
		return value, false, err
	}
}

func (wrapper RedisWrapper) SAdd(key, value string) error {
	return wrapper.rawClient.SAdd(key, value).Err()
}

func (wrapper RedisWrapper) SMembers(key string) (members []string, err error) {
	return wrapper.rawClient.SMembers(key).Result()
}

func (wrapper RedisWrapper) SRem(key, value string) (affected int64, err error) {
	return wrapper.rawClient.SRem(key, value).Result()
}

func (wrapper RedisWrapper) FlushDb() {
	wrapper.rawClient.FlushDB()
}
