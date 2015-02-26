package queue

import "github.com/adjust/goenv"

// TODO: move into goenv?
func SettingsFromGoenv(name string, goenv *goenv.Goenv) (nameOut, network, address string, db int) {
	network = goenv.Require("queue_redis.network")
	address = goenv.Require("queue_redis.address")
	db = goenv.GetInt("queue_redis.db", -1)
	return name, network, address, db
}
