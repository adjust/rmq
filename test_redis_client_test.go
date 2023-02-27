package rmq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTestRedisClient_Set(t *testing.T) {
	ctx := context.Background()
	type args struct {
		key        string
		value      string
		expiration time.Duration
	}
	tests := []struct {
		name   string
		client *TestRedisClient
		args   args
	}{
		{
			"successfull add",
			NewTestRedisClient(),
			args{
				"somekey",
				"somevalue",
				time.Duration(0),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// add
			err := tt.client.Set(ctx, tt.args.key, tt.args.value, tt.args.expiration)
			assert.NoError(t, err)

			// get
			v, err := tt.client.Get(ctx, tt.args.key)
			assert.Equal(t, tt.args.value, v)
			assert.NoError(t, err)

			// delete
			affected, err := tt.client.Del(ctx, tt.args.key)
			assert.Equal(t, int64(1), affected)
			assert.NoError(t, err)

			// delete it again
			affected, err = tt.client.Del(ctx, tt.args.key)
			assert.Equal(t, int64(0), affected)
			assert.NoError(t, err)
		})
	}
}

func TestTestRedisClient_SAdd(t *testing.T) {
	ctx := context.Background()
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name   string
		client *TestRedisClient
		args   args
	}{
		{
			"adding member",
			NewTestRedisClient(),
			args{
				"somekey",
				"somevalue",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			total, err := tt.client.SAdd(ctx, tt.args.key, tt.args.value)
			assert.NoError(t, err)
			assert.Equal(t, int64(1), total)

			total, err = tt.client.SAdd(ctx, tt.args.key, tt.args.value)
			assert.NoError(t, err)
			assert.Equal(t, int64(1), total)

			members, err := tt.client.SMembers(ctx, tt.args.key)
			assert.Equal(t, []string{tt.args.value}, members)
			assert.NoError(t, err)

			count, err := tt.client.SRem(ctx, tt.args.key, tt.args.value)
			assert.Equal(t, int64(1), count)
			assert.NoError(t, err)
		})
	}
}

func TestTestRedisClient_LPush(t *testing.T) {
	ctx := context.Background()
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name   string
		client *TestRedisClient
		args   args
		total  int64
	}{
		{
			"adding to list",
			NewTestRedisClient(),
			args{
				"somekey",
				"somevalue",
			},
			1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Push
			total, err := tt.client.LPush(ctx, tt.args.key, tt.args.value)
			assert.NoError(t, err)
			assert.Equal(t, tt.total, total)

			// Len
			count, err := tt.client.LLen(ctx, tt.args.key)
			assert.Equal(t, int64(1), count)
			assert.NoError(t, err)

			// Len of non-existing
			count, err = tt.client.LLen(ctx, tt.args.key+"nonsense")
			assert.Equal(t, int64(0), count)
			assert.NoError(t, err)

			// Lrem
			count, err = tt.client.LRem(ctx, tt.args.key, 100, tt.args.value)
			assert.Equal(t, int64(1), count)
			assert.NoError(t, err)

			// Len again
			count, err = tt.client.LLen(ctx, tt.args.key)
			assert.Equal(t, int64(0), count)
			assert.NoError(t, err)
		})
	}
}

func TestTestRedisClient_LPush_Len(t *testing.T) {
	ctx := context.Background()
	client := NewTestRedisClient()
	key := "list-key"

	total, err := client.LPush(ctx, key, "1", "2", "3")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), total)

	total, err = client.LPush(ctx, key, "4", "5", "6")
	assert.NoError(t, err)
	assert.Equal(t, int64(6), total)
}

func TestTestRedisClient_RPop(t *testing.T) {
	ctx := context.Background()
	client := NewTestRedisClient()
	key := "list-key"

	total, err := client.LPush(ctx, key, "1", "2", "3")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), total)

	value, err := client.RPop(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, "3", value)

	total, err = client.LLen(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), total)
}
