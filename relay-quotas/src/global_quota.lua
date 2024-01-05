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
--
-- The amount of budget we take from the counter depends on 3 things.
--  1: The configured default budget.
--  2: The amount of items being evaluated. If we evaluate a batch of items of a
--      higher quantity than the default budget, we increase the budget in order to not ratelimit
--      based on item quantity, as that's not the responsibility of this script.
--  3: Closeness to the given limit. The budget is capped so that it can not increase beyond the
--      quantity needed to reach the given limit.


-- The key to the global quota.
local key = KEYS[1]
-- The max amount that we want to take within the given slot. We won't take a budget if
-- the count is higher than the limit.
local limit = tonumber(ARGV[1])
-- When the redis key/val should be deleted. Should be current_time() + window + grace.
local expiry = tonumber(ARGV[2])
-- The minimum amount of budget needed for a specific batch.
local quantity = tonumber(ARGV[3])

local default_budget = 100

-- The reason to override the default max budget sometimes is to ensure the ratelimiter can handle
-- batches higher than `default_budget`.
local max_budget = math.max(quantity, default_budget)

local redis_count = tonumber(redis.call('GET', key) or 0)


if redis_count > limit then
    return {0, redis_count}
else
    -- Ensures the budget is not more than the quantity needed to hit the limit.
    local diff = limit - redis_count
    local budget = math.min(diff, max_budget)

    redis.call('INCRBY', key, budget)
    redis.call('EXPIREAT', key, expiry)

    return {budget, redis_count + budget}
end


