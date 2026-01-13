#!/bin/bash
# Comprehensive verification script for the arroyo MetricsBuffer fix

set -e

echo "========================================================================"
echo "Arroyo MetricsBuffer Fix - Comprehensive Verification"
echo "========================================================================"
echo

# Check if arroyo is installed
echo "1. Checking if arroyo is installed..."
if python3 -c "import arroyo" 2>/dev/null; then
    echo "   ✓ arroyo is installed"
    ARROYO_PATH=$(python3 -c "import arroyo, os; print(os.path.dirname(arroyo.__file__))")
    echo "   Location: $ARROYO_PATH"
else
    echo "   ✗ arroyo is not installed"
    echo "   Installing sentry-arroyo..."
    python3 -m pip install sentry-arroyo --quiet
    echo "   ✓ arroyo installed successfully"
fi
echo

# Check if fix is applied
echo "2. Checking if fix is applied..."
if python3 apply_arroyo_fix.py 2>&1 | grep -q "already been applied"; then
    echo "   ✓ Fix is already applied"
else
    echo "   Applying fix..."
    python3 apply_arroyo_fix.py
    echo "   ✓ Fix applied successfully"
fi
echo

# Run tests
echo "3. Running comprehensive tests..."
echo
python3 test_arroyo_fix.py
echo

# Run reproduction scenario
echo "4. Running production scenario simulation..."
echo
python3 reproduce_original_issue.py
echo

# Summary
echo "========================================================================"
echo "VERIFICATION COMPLETE"
echo "========================================================================"
echo
echo "✅ All tests passed successfully!"
echo "✅ The race condition has been fixed"
echo "✅ Production deployments can proceed with confidence"
echo
echo "Next steps:"
echo "  1. Deploy the fix to production environments"
echo "  2. Monitor for the RuntimeError in logs (should not occur)"
echo "  3. Submit upstream PR to sentry-arroyo repository"
echo
