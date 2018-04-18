package rmq

import (
	"strings"
	"testing"
	"time"
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
			if got := tt.client.Set(tt.args.key, tt.args.value, tt.args.expiration); got != tt.want {
				t.Errorf("TestRedisClient.Set() = %v, want %v", got, tt.want)
			}

			//get
			if strings.Compare(tt.client.Get(tt.args.key), tt.args.value) != 0 {
				t.Errorf("TestRedisClient.Get(%v) =, want %v", tt.args.key, tt.args.value)
			}

			//delete
			if affected, found := tt.client.Del(tt.args.key); affected != 1 || found != true {
				t.Errorf("TestRedisClient.Del(%v) =, want %v, %v", tt.args.key, 1, true)
			}

			//delete it again
			if affected, found := tt.client.Del(tt.args.key); affected != 0 || found != false {
				t.Errorf("TestRedisClient.Del(%v) =, want %v, %v", tt.args.key, 0, false)
			}
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
			if got := tt.client.SAdd(tt.args.key, tt.args.value); got != tt.want {
				t.Errorf("TestRedisClient.SAdd() = %v, want %v", got, tt.want)
			}

			if got := tt.client.SAdd(tt.args.key, tt.args.value); got != tt.want {
				t.Errorf("TestRedisClient.SAdd() = %v, want %v", got, tt.want)
			}

			if got := tt.client.SMembers(tt.args.key); len(got) != 1 || strings.Compare(got[0], tt.args.value) != 0 {
				t.Errorf("TestRedisClient.SMembers(%v) = %v, want %v", tt.args.key, got, []string{tt.args.value})
			}

			if got, ok := tt.client.SRem(tt.args.key, tt.args.value); got != 1 || ok != true {
				t.Errorf("TestRedisClient.SRem(%v, %v) = %v, %v, want %v, %v", tt.args.key, tt.args.value, got, ok, 1, true)
			}
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
			if got := tt.client.LPush(tt.args.key, tt.args.value); got != tt.want {
				t.Errorf("TestRedisClient.LPush() = %v, want %v", got, tt.want)
			}

			//Len
			if got, ok := tt.client.LLen(tt.args.key); got != 1 || ok != true {
				t.Errorf("TestRedisClient.LLen(%v) = %v, %v want %v, %v", tt.args.key, got, ok, 1, true)
			}

			//Len of non-existing
			if got, ok := tt.client.LLen(tt.args.key + "nonsense"); got != 0 || ok != true {
				t.Errorf("TestRedisClient.LLen(%vnonsense) = %v, %v want %v, %v", tt.args.key, got, ok, 0, true)
			}

			//Range
			if got := tt.client.LRange(tt.args.key, 0, 100); len(got) != 1 || strings.Compare(got[0], tt.args.value) != 0 {
				t.Errorf("TestRedisClient.LRange(%v, 0, 100) = %v want %v", tt.args.key, got, []string{tt.args.value})
			}

			//Lrem
			if got, ok := tt.client.LRem(tt.args.key, 100, tt.args.value); got != 1 || ok != true {
				t.Errorf("TestRedisClient.LRem(%v, 100, %v) = %v, %v want %v, %v", tt.args.key, tt.args.value, got, ok, 1, true)
			}

			//Len again
			if got, ok := tt.client.LLen(tt.args.key); got != 0 || ok != true {
				t.Errorf("TestRedisClient.LLen(%v) = %v, %v want %v, %v", tt.args.key, got, ok, 0, true)
			}
		})
	}
}
