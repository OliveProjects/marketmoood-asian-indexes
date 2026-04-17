#!/usr/bin/env python3
"""
Runs every 5 minutes.
Fetches intraday (1m/1d) and weekly (60m/5d) for Asian stock indexes.
"""

import json
import os
import time
from datetime import datetime, timezone

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"

SYMBOLS = {
    "Nikkei 225":          "%5EN225",
    "Hang Seng":           "%5EHSI",
    "Shanghai Composite":  "000001.SS",
    "KOSPI":               "%5EKS11",
    "Nifty 50":            "%5ENSEI",
    "ASX 200":             "%5EAXJO",
}


def save(path: str, data: object):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(path) // 1024
    print(f"  Saved {path} ({size_kb} KB)")


def fetch_yahoo_chart(symbol: str, interval: str, range_: str) -> list | None:
    try:
        r = requests.get(
            f"{YAHOO_BASE}{symbol}",
            params={"interval": interval, "range": range_},
            headers=HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        result = r.json()["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        return [
            {"x": int(ts) * 1000, "y": round(float(c), 4)}
            for ts, c in zip(timestamps, closes)
            if c is not None
        ]
    except Exception as e:
        print(f"    ERROR {symbol} {interval}/{range_}: {e}")
        return None


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"=== fetch_fast.py (asian-indexes)  {ts} ===")

    intraday: dict = {}
    weekly: dict = {}

    for name, symbol in SYMBOLS.items():
        print(f"  {name}")
        pts_i = fetch_yahoo_chart(symbol, "1m", "1d")
        if pts_i:
            intraday[name] = pts_i
        pts_w = fetch_yahoo_chart(symbol, "60m", "5d")
        if pts_w:
            weekly[name] = pts_w
        time.sleep(0.3)

    now_ms = int(time.time() * 1000)
    save("data/asian-intraday.json", {"fetched_at": now_ms, "assets": intraday})
    save("data/asian-weekly.json",   {"fetched_at": now_ms, "assets": weekly})

    print("=== Done ===")


if __name__ == "__main__":
    main()
