# Billing Tier Lookup Fix

Fixes SENTRY-3B06: Historical billing data tier lookup failures.

## Problem

Historical billing data contains reserved quantities for tiers that no longer exist in the current plan definition, causing `TierNotFound` errors when serializing billing history.

### Root Cause

1. `TierNotFound` is raised when serializing billing history
2. `get_tier()` cannot find a matching tier for the historical quantity (e.g., 10K transactions, 1GB attachments)
3. Historical `BillingMetricHistory` records contain reserved quantities from old plan tier configurations
4. Plan definitions (like `am1_sponsored`) have been modified over time - tiers added/removed without backward compatibility
5. The serializer uses current plan definitions to validate historical quantities, but those quantities are no longer valid tiers

### Example Scenario

1. Create a sponsored plan subscription with old tier values (e.g., 10K transactions)
2. Plan definition is later updated to only allow 10M transactions tier
3. Query `/api/0/customers/{org}/history/` to view past billing periods
4. Serializer attempts to calculate price using historical quantity against current plan tiers
5. `TierNotFound` raised because 10K is not a valid tier in current plan

## Solution

The fix implements a multi-strategy approach to gracefully handle historical tier lookups:

### 1. Store Historical Tier Data

Add fields to `BillingMetricHistory` to store the tier information at the time of billing:

```python
historical_tier_price: Optional[float] = None
historical_tier_name: Optional[str] = None
```

### 2. Graceful Tier Lookup in Serializer

The `BillingHistorySerializer` now implements multiple fallback strategies:

1. **Historical Data (preferred)**: Use stored historical tier price/name if available
2. **Exact Match**: Try to find exact tier in current plan definition
3. **Closest Match**: Find the closest tier that is <= the historical quantity
4. **Legacy Fallback**: Return a legacy tier placeholder if all else fails

### 3. Non-Strict Mode by Default

The serializer uses non-strict mode by default, which prevents `TierNotFound` errors from being raised during historical data serialization.

### 4. Data Migration Support

A `migrate_historical_tiers()` function is provided to populate historical tier data for existing records.

## Usage

### Basic Serialization

```python
from sentry_relay.billing import (
    BillingMetricHistory,
    BillingHistorySerializer,
)

# Historical record with tier that no longer exists
history = BillingMetricHistory(
    id=1,
    organization_id="org-123",
    plan_name="am1_sponsored",
    metric_type="transactions",
    reserved_quantity=10000,  # Old 10K tier
    actual_usage=8000,
    period_start="2023-01-01",
    period_end="2023-01-31",
    historical_tier_price=0.0008,  # Stored historical price
    historical_tier_name="10K transactions",
)

# Serialize without errors
serializer = BillingHistorySerializer()
data = serializer.serialize(history)

# The tier information will use historical data:
# {
#   "tier": {
#     "tier_match": "historical_data",
#     "is_historical": True,
#     "unit_price": 0.0008,
#     "name": "10K transactions",
#   },
#   "cost": 6.4,  # 8000 * 0.0008
# }
```

### Migrating Existing Records

```python
from sentry_relay.billing.serializers import migrate_historical_tiers

# Migrate a historical record
migrated = migrate_historical_tiers(history)

# This populates historical_tier_price and historical_tier_name
# based on current plan definition or closest match
```

### Strict Mode (for testing)

```python
# Use strict mode to raise TierNotFound for missing tiers
serializer = BillingHistorySerializer(use_strict_tier_lookup=True)

try:
    data = serializer.serialize(history_without_tier)
except TierNotFound as e:
    print(f"Tier not found: {e}")
```

## Implementation Details

### Tier Matching Strategies

The serializer's `_get_tier_info()` method implements the following strategies in order:

1. **historical_data**: Uses stored `historical_tier_price` and `historical_tier_name`
2. **exact**: Exact match found in current plan definition
3. **closest_fallback**: Closest tier <= historical quantity used as fallback
4. **legacy_fallback**: Legacy placeholder returned (only if non-strict mode)

Each serialized tier includes a `tier_match` field indicating which strategy was used.

### Cost Calculation

The `BillingMetricHistory.calculate_cost()` method:

1. First checks for `historical_tier_price` (preferred)
2. Attempts exact tier lookup in current plan
3. Falls back to closest tier if exact match not found
4. Returns 0.0 if no tier can be determined

### Plan Definitions

Plan definitions are stored in `PLAN_DEFINITIONS` dictionary in `models.py`:

```python
PLAN_DEFINITIONS = {
    "am1_sponsored": PlanDefinition(
        name="am1_sponsored",
        metric_type="transactions",
        tiers=[
            PlanTier(quantity=10000000, unit_price=0.00026, name="10M transactions"),
        ]
    ),
    "am1_sponsored_attachments": PlanDefinition(
        name="am1_sponsored_attachments",
        metric_type="attachments",
        tiers=[
            PlanTier(quantity=1000000000, unit_price=0.00000015, name="1GB attachments"),
        ]
    ),
}
```

## Testing

Run the test suite to verify the fix:

```bash
python3 test_billing_fix.py
```

The test suite includes:

- Tier lookup tests (exact match, non-strict, strict mode, closest tier)
- Cost calculation tests (historical price, exact tier, closest tier)
- Serialization tests (exact match, historical data, fallback strategies)
- Real-world scenarios from SENTRY-3B06

## Migration Checklist

When deploying this fix:

1. Deploy the new billing module code
2. Run data migration to populate historical tier data for existing records
3. Update API endpoints to use `BillingHistorySerializer`
4. Monitor for any remaining `TierNotFound` errors
5. Consider adding alerts for legacy_fallback tier matches (may indicate data quality issues)

## Benefits

- ✅ No more `TierNotFound` errors when viewing historical billing data
- ✅ Backward compatible with historical plan configurations
- ✅ Preserves accurate historical pricing information
- ✅ Graceful degradation for edge cases
- ✅ Clear indication of which matching strategy was used
- ✅ Data migration support for existing records
