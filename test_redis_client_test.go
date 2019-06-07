package rmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTestRedisClient_Set(t *testing.T) {
	type args struct {
		key        string
		value      string
		expiration time.Duration
	}
	tests := []struct {
		name   string
		client *TestRedisClient
		args   args
		want   bool
	}{
		{
			"successfull add",
			NewTestRedisClient(),
			args{
				"somekey",
				"somevalue",
				time.Duration(0),
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			//add
			got, err := tt.client.Set(tt.args.key, tt.args.value, tt.args.expiration)
			assert.Equal(t, tt.want, got)
			assert.NoError(t, err)

			//get
			v, err := tt.client.Get(tt.args.key)
			assert.Equal(t, tt.args.value, v)
			assert.NoError(t, err)

			//delete
			affected, found, err := tt.client.Del(tt.args.key)
			assert.Equal(t, 1, affected)
			assert.True(t, found)
			assert.NoError(t, err)

			//delete it again
			affected, found, err = tt.client.Del(tt.args.key)
			assert.Equal(t, 0, affected)
			assert.False(t, found)
			assert.NoError(t, err)
		})
	}
}

func TestTestRedisClient_SAdd(t *testing.T) {
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name   string
		client *TestRedisClient
		args   args
		want   bool
	}{
		{
			"adding member",
			NewTestRedisClient(),
			args{
				"somekey",
				"somevalue",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.client.SAdd(tt.args.key, tt.args.value)
			assert.Equal(t, tt.want, got)
			assert.NoError(t, err)

			got, err = tt.client.SAdd(tt.args.key, tt.args.value)
			assert.Equal(t, tt.want, got)
			assert.NoError(t, err)

			members, err := tt.client.SMembers(tt.args.key)
			assert.Equal(t, []string{tt.args.value}, members)
			assert.NoError(t, err)

			count, ok, err := tt.client.SRem(tt.args.key, tt.args.value)
			assert.Equal(t, 1, count)
			assert.True(t, ok)
			assert.NoError(t, err)
		})
	}
}

func TestTestRedisClient_LPush(t *testing.T) {
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name   string
		client *TestRedisClient
		args   args
		want   bool
	}{
		{
			"adding to list",
			NewTestRedisClient(),
			args{
				"somekey",
				"somevalue",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			//Push
			got, err := tt.client.LPush(tt.args.key, tt.args.value)
			assert.Equal(t, tt.want, got)
			assert.NoError(t, err)

			//Len
			count, ok, err := tt.client.LLen(tt.args.key)
			assert.Equal(t, 1, count)
			assert.True(t, ok)
			assert.NoError(t, err)

			//Len of non-existing
			count, ok, err = tt.client.LLen(tt.args.key + "nonsense")
			assert.Equal(t, 0, count)
			assert.True(t, ok)
			assert.NoError(t, err)

			//Range
			members, err := tt.client.LRange(tt.args.key, 0, 100)
			assert.Equal(t, []string{tt.args.value}, members)
			assert.NoError(t, err)

			//Lrem
			count, ok, err = tt.client.LRem(tt.args.key, 100, tt.args.value)
			assert.Equal(t, 1, count)
			assert.True(t, ok)
			assert.NoError(t, err)

			//Len again
			count, ok, err = tt.client.LLen(tt.args.key)
			assert.Equal(t, 0, count)
			assert.True(t, ok)
			assert.NoError(t, err)
		})
	}
}
