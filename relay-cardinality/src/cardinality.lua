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

local function batches(n, batch_size, offset, max_items)
    local i = 0
    if max_items then
        n = math.min(n, max_items + offset)
    end

    return function()
        local from = i * batch_size + 1 + offset
        i = i + 1
        if from <= n then
            local to = math.min(from + batch_size - 1, n)

            return from, to
        end
    end
end


local ACCEPTED = true
local REJECTED = false
local HASHES_OFFSET = 2 -- arg1: max_cardinality, arg2: expiry

local working_set = KEYS[1]
local max_cardinality = tonumber(ARGV[1])
local expire = tonumber(ARGV[2])
local num_hashes = #ARGV - HASHES_OFFSET

local results = {
    0, -- total cardinality
}

-- Adds elements from the table `t`, starting with index `offset` and a maximum of `max` elements
-- to all sets in `KEYS`.
--
-- Returns the total amount of values added to the 'working set'.
local function sadd(t, offset, max)
    local added = 0;

    for i = 1, #KEYS do
        local is_working_set = i == 1

        for from, to in batches(#t, 7000, offset, max) do
            local r = redis.call('SADD', KEYS[i], unpack(t, from, to))
            if is_working_set then
                added = added + r
            end
        end
    end

    return added
end

-- Bumps to expiry of all sets by the passed expiry
local function bump_expiry()
    for _, key in ipairs(KEYS) do
        redis.call('EXPIRE', key, expire)
    end
end


local current_cardinality = redis.call('SCARD', working_set) or 0
local budget = math.max(0, max_cardinality - current_cardinality)


-- Fast Path: we have enough budget to fit all elements
if budget >= num_hashes then
    local added = sadd(ARGV, HASHES_OFFSET)
    -- New current cardinality is current + amount of keys that have been added to the set
    current_cardinality = current_cardinality + added

    for _ = 1, num_hashes do
        table.insert(results, ACCEPTED)
    end

    if added > 0 then
        bump_expiry()
    end

    results[1] = current_cardinality
    return results
end

-- We do have some budget
local offset = HASHES_OFFSET
local needs_expiry_bumped = false
while budget > 0 and offset < #ARGV do
    local len = math.min(#ARGV - offset, budget)
    local added = sadd(ARGV, offset, len)

    current_cardinality = current_cardinality + added
    needs_expiry_bumped = needs_expiry_bumped or added > 0

    for _ = 1, len do
        table.insert(results, ACCEPTED)
    end

    offset = offset + len
    budget = budget - added
end

-- Update cardinality, at this point cardinality cannot change anymore,
-- the maximum amount of keys that can be added, have been added to the set.
results[1] = current_cardinality

-- If we ran out of budget, check the remaining items for membership
if budget <= 0 and offset < #ARGV then
    for arg_i = offset + 1, #ARGV do
        local value = ARGV[arg_i]

        if redis.call('SISMEMBER', working_set, value) == 1 then
            table.insert(results, ACCEPTED)
        else
            table.insert(results, REJECTED)
        end
    end
end

if needs_expiry_bumped then
    bump_expiry()
end

return results
