-- Check a collection of quota counters to identify if an item should be rate
-- limited. For each quota, repeat the same set of ``KEYS`` and ``ARGV``:
--
-- ``KEYS`` (2 per quota):
--  * [string] Key of the counter.
--  * [string] Key of the refund counter.
--
-- ``ARGV`` (4 per quota):
--  * [number]  Quota limit. Can be ``-1`` for unlimited quotas.
--  * [number]  Absolute Expiration time as Unix timestamp (secs since 1.1.1970 ) for the key.
--  * [number]  Quantity to increment the quota by, or ``0`` to check without incrementing.
--  * [boolean] If set to `true` - reject only if the previous update already reached the limit.
--
-- For example, to check the following two quotas each with a timeout of 10 minutes from now:
--  * Key ``foo``, refund key ``foo_refund``, limit ``10``; quantity ``5``
--  * Key ``bar``, refund key ``bar_refund``, limit ``20``; quantity ``1``
--
-- Send these values:
--
--     KEYS = {"foo", "foo_refund", "bar", "bar_refund"}
--     ARGV = {10, 600 + now(), 5, false, 20, 600 + now(), 1, true}
--
-- The script applies the following logic:
--  * If all checks pass, the item is accepted and the counters for all quotas
--    are incremented.
--  * If any check fails, the item is rejected and the counters for all remain
--    unchanged.
--
-- The result is a Lua table/array (Redis multi bulk reply) that specifies
-- whether or not the item was *rejected* based on the provided limit.
local NUM_KEYS = 2
local NUM_ARGS = 5
assert(#KEYS % NUM_KEYS == 0, "there must be 2 keys per quota")
assert(#ARGV % NUM_ARGS == 0, "there must be 5 args per quota")
assert(#KEYS / NUM_KEYS == #ARGV / NUM_ARGS, "incorrect number of keys and arguments provided")

local function read_from_redis(redis_key, dims_key)
    local value = 0
    local result = redis.pcall('HGET', redis_key, dims_key)
    if type(result) == "table" and result['err'] ~= nil then
        value = redis.call('GET', redis_key) or 0
        redis.call('DEL', redis_key)

        -- Overkill?  Maybe we just let the key stay deleted, give everyone
        -- a little free quota during the changeover.
        redis.call('HSET', redis_key, dims_key, value)
    else
        value = result or 0
    end
    return value
end

local results = {}
local failed = false
local num_quotas = #KEYS / NUM_KEYS
for i = 0, num_quotas - 1 do
    local k = i * NUM_KEYS + 1
    local v = i * NUM_ARGS + 1

    local limit = tonumber(ARGV[v])
    local quantity = tonumber(ARGV[v + 2])
    local over_accept_once = ARGV[v + 3]
    local dims_key = ARGV[v + 4]

    local redis_key = KEYS[k]
    local refund_key = KEYS[k + 1]

    local main_value = read_from_redis(redis_key, dims_key)
    local refund_value = read_from_redis(refund_key, dims_key)

    local consumed = main_value - refund_value

    local rejected = false;
    -- limit=-1 means "no limit"
    if limit >= 0 then
        -- Without over_accept_once, we never increment past the limit. if quantity is 0, check instead if we reached limit.
        -- With over_accept_once, we only reject if the previous update already reached the limit.
        -- This way, we ensure that we increment to or past the limit at some point,
        -- such that subsequent checks with quantity=0 are actually rejected.
        --
        -- NOTE: redis-rs crate since version 0.18.0 (2020-12-03) passes '1' in case of true and '0' when false.
        if quantity == 0 or over_accept_once == '1' then
            rejected = consumed >= limit
        else
            rejected = consumed + quantity > limit
        end
    end

    failed = failed or rejected

    table.insert(results, rejected)
    table.insert(results, consumed)
end

if not failed then
    for i = 0, num_quotas - 1 do
        local k = i * NUM_KEYS + 1
        local v = i * NUM_ARGS + 1

        local quantity = tonumber(ARGV[v + 2])
        local expiry = ARGV[v + 1]
        local dims = ARGV[v + 4]
        local redis_key = KEYS[k]

        if quantity > 0 then
            redis.call('HINCRBY', redis_key, dims, quantity)

            -- With redis 7.X, we can specify 'NX' so that we avoid re-setting the
            -- same expiry, but I don't know if this will be significantly cheaper.
            -- Could also do an explicit TTL check to see if the key has a TTL, only
            -- setting an expiry then (again, if it's the actual setting that's expensive,
            -- and not the redis.call itself.)
            redis.call('EXPIREAT', redis_key, expiry)

            -- Adjust the consumed value with the just increased quantity.
            results[k + 1] = results[k + 1] + quantity;
        end
    end
end

return results
