package rmq

import (
	"time"

	"github.com/go-redis/redis/v7"
)

type RedisWrapper struct {
	rawClient *redis.Client
}

// TODO: return bool return value?
func (wrapper RedisWrapper) Set(key string, value string, expiration time.Duration) (ok bool, err error) {
	return checkErr(wrapper.rawClient.Set(key, value, expiration).Err())
}

func (wrapper RedisWrapper) Del(key string) (affected int, ok bool, err error) {
	n, err := wrapper.rawClient.Del(key).Result()
	ok, err = checkErr(err)
	if !ok {
		return 0, false, err
	}
	return int(n), ok, nil
}

func (wrapper RedisWrapper) TTL(key string) (ttl time.Duration, ok bool, err error) {
	ttl, err = wrapper.rawClient.TTL(key).Result()
	ok, err = checkErr(err)
	if !ok {
		return 0, false, err
	}
	return ttl, ok, nil
}

func (wrapper RedisWrapper) LPush(key string, value ...string) (ok bool, err error) {
	return checkErr(wrapper.rawClient.LPush(key, value).Err())
}

func (wrapper RedisWrapper) LLen(key string) (affected int, ok bool, err error) {
	n, err := wrapper.rawClient.LLen(key).Result()
	ok, err = checkErr(err)
	if !ok {
		return 0, false, err
	}
	return int(n), ok, nil
}

func (wrapper RedisWrapper) LRem(key string, count int, value string) (affected int, ok bool, err error) {
	n, err := wrapper.rawClient.LRem(key, int64(count), value).Result()
	ok, err = checkErr(err)
	return int(n), ok, err
}

func (wrapper RedisWrapper) LTrim(key string, start, stop int) (ok bool, err error) {
	return checkErr(wrapper.rawClient.LTrim(key, int64(start), int64(stop)).Err())
}

func (wrapper RedisWrapper) RPopLPush(source, destination string) (value string, ok bool, err error) {
	value, err = wrapper.rawClient.RPopLPush(source, destination).Result()
	ok, err = checkErr(err)
	return value, ok, err
}

func (wrapper RedisWrapper) SAdd(key, value string) (ok bool, err error) {
	return checkErr(wrapper.rawClient.SAdd(key, value).Err())
}

func (wrapper RedisWrapper) SMembers(key string) (members []string, err error) {
	members, err = wrapper.rawClient.SMembers(key).Result()
	if ok, err := checkErr(err); !ok {
		return []string{}, err
	}
	return members, nil
}

func (wrapper RedisWrapper) SRem(key, value string) (affected int, ok bool, err error) {
	n, err := wrapper.rawClient.SRem(key, value).Result()
	ok, err = checkErr(err)
	if !ok {
		return 0, false, err
	}
	return int(n), ok, nil
}

func (wrapper RedisWrapper) FlushDb() {
	wrapper.rawClient.FlushDB()
}

// checkErr returns true if there is no error, false if the result error is nil and panics if there's another error
// TODO: remove this and check redis.Nil outside?
func checkErr(err error) (bool, error) {
	switch err {
	case nil:
		return true, nil
	case redis.Nil:
		return false, nil
	default:
		return false, err // TODO: Wrap?
	}
}
