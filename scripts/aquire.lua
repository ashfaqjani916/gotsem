local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local expiry = tonumber(ARGV[2])
local slotID = ARGV[3]
local max    = tonumber(ARGV[4])

-- evict expired slots (leaked from crashed pods/disconnected clients)
redis.call("ZREMRANGEBYSCORE", key, "-inf", now)

local count = tonumber(redis.call("ZCARD", key))

if count >= max then
  return { 0, count }
end

redis.call("ZADD", key, expiry, slotID)
-- key TTL = slotTTL + 10s buffer so key doesn't vanish before last slot expires
redis.call("PEXPIRE", key, tonumber(ARGV[5]))
return { 1, count + 1 }
