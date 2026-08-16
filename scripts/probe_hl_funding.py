"""Probe Hyperliquid fundingHistory API to confirm available fields."""
import json
import time

import requests

r = requests.post(
    "https://api.hyperliquid.xyz/info",
    json={
        "type": "fundingHistory",
        "coin": "BTC",
        "startTime": int(time.time() * 1000) - 3 * 86400 * 1000,
    },
    timeout=15,
)
r.raise_for_status()
data = r.json()
print("count:", len(data))
print("keys in first item:", list(data[0].keys()) if data else "EMPTY")
print("samples:")
for item in data[-3:]:
    print(" ", json.dumps(item))
