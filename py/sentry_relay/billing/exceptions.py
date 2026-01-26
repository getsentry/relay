"""Billing-related exceptions.

Fixes SENTRY-3B06: Historical billing data tier lookup failures.
"""


class TierNotFound(Exception):
    """Exception raised when a tier cannot be found for a given quantity.
    
    This can occur when historical billing data references tier quantities
    that no longer exist in the current plan definition.
    """
    
    def __init__(self, quantity, plan_name=None):
        self.quantity = quantity
        self.plan_name = plan_name
        message = f"Tier not found for quantity: {quantity}"
        if plan_name:
            message += f" in plan: {plan_name}"
        super().__init__(message)
