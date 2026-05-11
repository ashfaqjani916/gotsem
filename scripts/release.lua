local key    = KEYS[1]
local slotID = ARGV[1]
local now    = tonumber(ARGV[2])

redis.call("ZREMRANGEBYSCORE", key, "-inf", now) -- clean expired while we're here
redis.call("ZREM", key, slotID)
return redis.call("ZCARD", key)
