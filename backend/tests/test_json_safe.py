import json
from datetime import datetime
from decimal import Decimal
import ipaddress

from ai_crypto_trader.services.paper_trader.maintenance import json_safe


def test_json_safe_serializes_objects():
    payload = {
        "ip": ipaddress.ip_address("127.0.0.1"),
        "amount": Decimal("1.23"),
        "ts": datetime(2024, 1, 1, 0, 0, 0),
    }
    safe = json_safe(payload)
    json.dumps(safe)
    assert safe["ip"] == "127.0.0.1"
    assert safe["amount"] == "1.23"
