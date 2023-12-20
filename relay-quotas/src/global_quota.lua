
-- Global quota system.
--
-- Made to work with a local cache of quota limit. The caller will "take" a certain contingency
-- from the global counter, given that the limit have not been exceeded. This is to dramatically
-- reduce the amount of redis calls while at the same time not overshooting the quota.
-- We return both the size of the contingency and the size of the redis count.
-- The reason we return the size of the redis count is to avoid asking for more contingency
-- if the previously seen redis count is bigger than the limit.




-- The maximum contingency we will take. We will take less if we are too close to the limit.
local max_contingency = 100
-- The key to the global quota.
local key = KEYS[1]
-- The max amount that we want to take within the given slot. We won't take a contingency if
-- the count is higher than the limit.
local limit = tonumber(ARGV[1])
-- When the redis key/val should be deleted. Should be current_time() + window + grace.
local expiry = tonumber(ARGV[2])

local redis_count = tonumber(redis.call('GET', key))

if redis_count == nil then
    redis.call('SET', key, 0)
    redis_count = 0
end

if redis_count > limit then
    return 0, redis_count
else
    local diff = limit - redis_count
    local contingency = math.min(diff, max_contingency)

    redis.call('INCRBY', key, contingency)
    redis.call('EXPIREAT', key, expiry)
    return  contingency, redis_count + contingency
end


