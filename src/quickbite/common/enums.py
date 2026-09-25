"""Domain enumerations."""

import enum


class OrderStatus(enum.StrEnum):
    """Lifecycle of a food delivery order."""

    PENDING = "PENDING"  # Accepted by the API, waiting for payment.
    PAID = "PAID"  # Payment succeeded, waiting for restaurant confirmation.
    CONFIRMED = "CONFIRMED"  # Restaurant confirmed, waiting for driver assignment.
    DRIVER_ASSIGNED = "DRIVER_ASSIGNED"  # Driver assigned; happy-path terminal state.
    FAILED = "FAILED"  # Payment failed permanently; terminal state.


class PaymentBehavior(enum.StrEnum):
    """Demo hook controlling simulated payment outcome for a single order."""

    AUTO = "auto"  # Use the worker's PAYMENT_FAILURE_MODE setting.
    NEVER_FAIL = "never_fail"  # Payment always succeeds.
    ALWAYS_FAIL = "always_fail"  # Payment always fails (drives retries -> DLQ).
    FAIL_TWICE = "fail_twice"  # Fails the first two attempts, then succeeds.
