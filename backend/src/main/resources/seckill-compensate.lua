local stockKey = KEYS[1]
local orderKey = KEYS[2]
local userId = ARGV[1]

if redis.call('SREM', orderKey, userId) == 1 then
    redis.call('INCRBY', stockKey, 1)
    return 1
end

return 0
