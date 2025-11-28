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
assert(#KEYS % 2 == 0, "there must be 2 keys per quota")
assert(#ARGV % 4 == 0, "there must be 4 args per quota")
assert(#KEYS / 2 == #ARGV / 4, "incorrect number of keys and arguments provided")

local all_values = redis.call('MGET', unpack(KEYS))

local results = {}
local failed = false
local num_quotas = #KEYS / 2
for i = 0, num_quotas - 1 do
    local k = i * 2 + 1
    local v = i * 4 + 1

    local limit = tonumber(ARGV[v])
    local quantity = tonumber(ARGV[v + 2])
    local over_accept_once = ARGV[v + 3]

    local main_value = all_values[k] or 0
    local refund_value = all_values[k + 1] or 0
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
        local k = i * 2 + 1
        local v = i * 4 + 1

        local quantity = tonumber(ARGV[v + 2])
        local expiry = ARGV[v + 1]

        if quantity > 0 then
            if redis.call('INCRBY', KEYS[k], quantity) == quantity then
                -- Only expire on the first invocation of `INCRBY`.
                redis.call('EXPIREAT', KEYS[k], expiry)
            end

            -- Adjust the consumed value with the just increased quantity.
            results[k + 1] = results[k + 1] + quantity;
        end
    end
end

return results
