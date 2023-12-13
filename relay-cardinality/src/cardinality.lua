-- Check the cardinality of a list of hashes/strings and return wether the item should be dropped.
--
--
-- ``KEYS``: A list of cardinality sets, the first key 'working set' is used to check cardinality,
-- hashes will be updated in all passed keys.
--
-- ``ARGV``:
--  * [number] Max cardinality.
--  * [number] Set expiry.
--  * [string...] List of hashes.
--
--  Returns a table, the first element in the table contains the new set cardinality and for
--  every passed hash (in order) the table contains `0` if the hash was rejected and `1` if it was accepted.
--
--
-- For example to check the cardinality limit of 3 with the hashes `1, 2, 3, 4, 5` 
-- in a sliding window of 1 hour with a granularity of 2 (sets: `0` and `1800`)
-- send the following values:
--
--      KEYS: { "prefix-scope-0", "prefix-scope-1800" }
--      ARGV: { 
--          3,             -- Limit
--          3600,          -- Window size / Expiry
--          1, 2, 3, 4, 5  -- Hashes
--      }
--
-- The script returns:
--
--      {
--          3, -- new cardinality
--          1, -- accepted: 1
--          1, -- accepted: 2
--          1, -- accepted: 3
--          0, -- rejected: 4
--          0, -- rejected: 5
--      }
--
-- Redis state after execution:
--
--      prefix-scope-0:    {1, 2, 3} | Expires: now + 3600
--      prefix-scope-1800: {1, 2, 3} | Expires: now + 3600
-- 
--
-- The script applies the following logic for every hash passed as an argument:
--  * if the cardinality has not been reached yet, the hash is added to all sets
--    and the item is marked as accepted
--  * otherwise it only marks the hash as accepted when the hash was already seen before, 
--    this is done by checking wether the hash is contained in the 'working set'
-- 
-- Afterwards if any item was added to the 'working set' the expiry of all sets is bumped
-- with the passed expiry.

local ACCEPTED = true
local REJECTED = false

local working_set = KEYS[1]
local max_cardinality = tonumber(ARGV[1])
local expire = tonumber(ARGV[2])

local results = {
    0, -- total cardinality
}

local current_cardinality = redis.call('SCARD', working_set) or 0
-- Only need to update expiry if a set was actually changed
local needs_expire = false

for arg_i = 3, #ARGV do
    local value = ARGV[arg_i]
    if current_cardinality < max_cardinality then
        local is_new_item = redis.call('SADD', working_set, value) == 1

        table.insert(results, ACCEPTED)

        if is_new_item then
            current_cardinality = current_cardinality + 1
            needs_expire = true
        end

        -- add to the remainings sets
        for i = 2, #KEYS do
            redis.call('SADD', KEYS[i], value)
        end
    elseif redis.call('SISMEMBER', working_set, value) == 1 then
        table.insert(results, ACCEPTED)
    else
        table.insert(results, REJECTED)
    end
end

if needs_expire then
    for _, key in ipairs(KEYS) do
        redis.call('EXPIRE', key, expire)
    end
end

results[1] = current_cardinality

return results
