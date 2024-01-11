-- Global quota system.
--
-- Made to work with a local cache of quota limit. The caller will "take" a certain budget
-- from the global redis counter, given that the limit have not been exceeded. This is to dramatically
-- reduce the amount of redis calls while at the same time not overshooting the quota.
-- We return both the size of the taken budget and the size of the redis count.
-- The reason we return the size of the redis count is to avoid asking for more budget when
-- the previously seen redis count is still bigger than the limit.
--
-- The redis keys are unique to their timeslot, which is why we let them expire in order to not
-- fill up redis with dead keys.


-- The key to the global quota.
local key = KEYS[1]
-- The max amount that we want to take within the given slot. We won't take a budget if
-- the count is higher than the limit.
local limit = tonumber(ARGV[1])
-- When the redis key/val should be deleted.
local expiry = tonumber(ARGV[2])
-- The budget that the caller intends to take. Will be capped if too close to the limit.
local requested_budget = tonumber(ARGV[3])

local redis_count = tonumber(redis.call('GET', key) or 0)

if limit == -1 then
    limit = math.huge
end

if redis_count >= limit then
    return {0, redis_count}
else
    -- Ensures the budget is not more than the quantity needed to hit the limit.
    local headroom = limit - redis_count
    local budget = math.min(headroom, requested_budget)
    redis.call('INCRBY', key, budget)
    if redis_count == 0 then
        redis.call('EXPIREAT', key, expiry)
    end

    return {budget, redis_count + budget}
end


