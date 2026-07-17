local windowKey = KEYS[1]
local now = tonumber(ARGV[1])
local windowMillis = tonumber(ARGV[2])
local requestId = ARGV[3]
local ttlSeconds = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', windowKey, 0, now - windowMillis)
redis.call('ZADD', windowKey, now, requestId)
redis.call('EXPIRE', windowKey, ttlSeconds)

return redis.call('ZCARD', windowKey)
