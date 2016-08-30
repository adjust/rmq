package rmq

import "gopkg.in/redis.v3"

var (
	luaConsumeScript = redis.NewScript(`
				local num = tonumber(KEYS[3])
				local payload = redis.call("LRANGE", KEYS[1], -num, -1)
				if #payload == 0 then return payload end
				redis.call("LTRIM", KEYS[1], 0, -(num+1))
				redis.call("LPUSH", KEYS[2], unpack(payload))
				return payload`)
)
