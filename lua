
-- 获取锁中的线程标识 get key
local id = redis.call('get', KEYS[1])

-- 比较两者是否一致
if (id == tARGV[1]) then
    -- 释放锁 del key
    return redis.call('del', KEYS[1])
end
return 0