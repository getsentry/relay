-- Check a collection of quota counters to identify if an item should be rate
-- limited. For each quota, repeat the same set of ``KEYS`` and ``ARGV``:
--
-- ``KEYS`` (2 per quota):
--  * [string] Key of the counter.
--  * [string] Key of the refund counter.
--
-- ``ARGV`` (3 per quota):
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

-- parse the incoming string into boolean, returns `nil` if could not parse which is also considered `false`
local function parse_boolean(v)
    if v == '1' or v == 'true' or v == 'TRUE' then
        return true
    elseif v == '0' or v == 'false' or v == 'FALSE' then
        return false
    else
        return nil
    end
end

local results = {}
local failed = false
local num_quotas = #KEYS / 2
for i=0, num_quotas - 1 do
    local k = i * 2 + 1
    local v = i * 4 + 1

    local limit = tonumber(ARGV[v])
    local quantity = tonumber(ARGV[v+2])
    local over_accept_once = parse_boolean(ARGV[v+3])
    local rejected = false
    -- limit=-1 means "no limit"
    if limit >= 0 then
        local consumed = (redis.call('GET', KEYS[k]) or 0) - (redis.call('GET', KEYS[k + 1]) or 0)
        -- Without over_accept_once, we never increment past the limit. if quantity is 0, check instead if we reached limit.
        -- With over_accept_once, we only reject if the previous update already reached the limit. 
        -- This way, we ensure that we increment to or past the limit at some point,
        -- such that subsequent checks with quantity=0 are actually rejected.
        if quantity == 0 or over_accept_once then
            rejected = consumed >= limit
        else
            rejected = consumed + quantity > limit
        end
    end

    if rejected then
        failed = true
    end
    results[i + 1] = rejected
end

if not failed then
    for i=0, num_quotas - 1 do
        local k = i * 2 + 1
        local v = i * 4 + 1

        if tonumber(ARGV[v + 2]) > 0 then
            redis.call('INCRBY', KEYS[k], ARGV[v + 2])
            redis.call('EXPIREAT', KEYS[k], ARGV[v + 1])
        end
    end
end

return results
