--1.参数列表
--1.1.优惠券id
local voucherid = ARGV[1]
--1.2.用户id
local userid = ARGV[2]
--1.3.订单id
local orderid = ARGV[3]
local now = tonumber(ARGV[4])

--2.Key列表
--2.1.库存key
local stockKey = 'seckill:stock:' .. voucherid
--2.2.订单key
local orderKey = 'seckill:order:' .. voucherid
local metaKey = 'seckill:meta:' .. voucherid

--3.脚本业务
local beginTime = redis.call('HGET', metaKey, 'begin')
local endTime = redis.call('HGET', metaKey, 'end')
if (not beginTime or not endTime or now < tonumber(beginTime)) then
    return 3
end
if (now > tonumber(endTime)) then
    return 4
end

--3.1.判断库存是否充足
local stock = redis.call('get', stockKey)
if (not stock or tonumber(stock) <= 0) then
    --3.1.1库存不足
    return 1
end

--3.2.判断是否已经买过
if (redis.call('sismember', orderKey, userid) == 1) then
    --3.2.1已经买过
    return 2
end

--3.3.库存减1，把用户信息存进set
redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userid)
--3.4.发送消息到队列中
redis.call('XADD', 'stream.orders', '*', 'userId', userid,  'voucherId', voucherid, 'id', orderid)
return 0
