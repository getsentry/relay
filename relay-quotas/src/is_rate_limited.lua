-- Check a collection of quota counters to identify if an item should be rate
-- limited. For each quota, repeat the same set of ``KEYS`` and ``ARGV``:
--
-- ``KEYS`` (2 per quota):
--  * [string] Key of the counter.
--  * [string] Key of the refund counter.
--
-- ``ARGV`` (3 per quota):
--  * [number] Quota limit. Can be ``-1`` for unlimited quotas.
--  * [number] Absolute Expiration time as Unix timestamp (secs since 1.1.1970 ) for the key.
--  * [number] Quantity to increment the quota by.
--
-- For example, to check the following two quotas each with a timeout of 10 minutes from now:
--  * Key ``foo``, refund key ``foo_refund``, limit ``10``; quantity ``5``
--  * Key ``bar``, refund key ``bar_refund``, limit ``20``; quantity ``1``
--
-- Send these values:
--
--     KEYS = {"foo", "foo_refund", "bar", "bar_refund"}
--     ARGV = {10, 600 + now(), 5, 20, 600 + now(), 1}
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
assert(#ARGV % 3 == 0, "there must be 3 args per quota")
assert(#KEYS / 2 == #ARGV / 3, "incorrect number of keys and arguments provided")

local results = {}
local failed = false
local num_quotas = #KEYS / 2
for i=0, num_quotas - 1 do
    local k = i * 2 + 1
    local v = i * 3 + 1

    local limit = tonumber(ARGV[v])
    local quantity = tonumber(ARGV[v+2])
    local rejected = false
    -- limit=-1 means "no limit"
    if limit >= 0 then
        rejected = (redis.call('GET', KEYS[k]) or 0) - (redis.call('GET', KEYS[k + 1]) or 0) + quantity > limit
    end

    if rejected then
        failed = true
    end
    results[i + 1] = rejected
end

if not failed then
    for i=0, num_quotas - 1 do
        local k = i * 2 + 1
        local v = i * 3 + 1

        redis.call('INCRBY', KEYS[k], ARGV[v + 2])
        redis.call('EXPIREAT', KEYS[k], ARGV[v + 1])
    end
end

return results
