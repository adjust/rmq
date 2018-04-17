package rmq

import "time"

type TestRedisClient struct {
	// ...
}

func NewTestRedisClient() *TestRedisClient {
	return &TestRedisClient{
		// ...
	}
}

func (client *TestRedisClient) Set(key string, value string, expiration time.Duration) bool {
	return false
}

func (client *TestRedisClient) Del(key string) (affected int, ok bool) {
	return 0, false
}

func (client *TestRedisClient) TTL(key string) (ttl time.Duration, ok bool) {
	return 0, false
}

func (client *TestRedisClient) LPush(key, value string) bool {
	return false
}

func (client *TestRedisClient) LLen(key string) (affected int, ok bool) {
	return 0, false
}

func (client *TestRedisClient) LRem(key string, count int, value string) (affected int, ok bool) {
	return 0, false
}

func (client *TestRedisClient) LTrim(key string, start, stop int) {
}

func (client *TestRedisClient) RPopLPush(source, destination string) (value string, ok bool) {
	return "", false
}

func (client *TestRedisClient) SAdd(key, value string) bool {
	return false
}

func (client *TestRedisClient) SMembers(key string) (members []string) {
	return nil
}

func (client *TestRedisClient) SRem(key, value string) (affected int, ok bool) {
	return 0, false
}

func (client *TestRedisClient) FlushDb() {
}
