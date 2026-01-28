"""
ORM models that are not part of the shared common.models module.
"""

# Re-export shared DB models when needed by external imports.
from ai_crypto_trader.common.models import NotificationOutbox  # noqa: F401
