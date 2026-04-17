#!/usr/bin/env python3
"""
Runs once daily after Asian market close (~11:00 UTC).
Fetches full price history for Asian indexes (Stooq + Yahoo fallback).
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
STOOQ_BASE = "https://stooq.com/q/d/l/"

# name → (stooq_symbol or None, yahoo_symbol)
INDEXES = {
    "Nikkei 225":         ("^nkx",   "%5EN225"),
    "Hang Seng":          ("^hsi",   "%5EHSI"),
    "Shanghai Composite": (None,     "000001.SS"),
    "KOSPI":              (None,     "%5EKS11"),
    "Nifty 50":           ("^nsei",  "%5ENSEI"),
    "ASX 200":            ("^axjo",  "%5EAXJO"),
}


def save(path: str, data: object):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(path) // 1024
    print(f"  Saved {path} ({size_kb} KB)")


def parse_stooq_csv(text: str) -> list:
    lines = text.strip().splitlines()
    result = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < 5:
            continue
        try:
            dt = datetime.strptime(parts[0].strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
            close = float(parts[4].strip())
            result.append({"x": int(dt.timestamp() * 1000), "y": close})
        except (ValueError, IndexError):
            continue
    return sorted(result, key=lambda p: p["x"])


def fetch_live_yahoo(symbol: str) -> tuple:
    try:
        r = requests.get(
            f"{YAHOO_BASE}{symbol}",
            params={"interval": "1m", "range": "1d"},
            headers=HEADERS, timeout=15,
        )
        r.raise_for_status()
        meta = r.json()["chart"]["result"][0]["meta"]
        return (
            meta.get("regularMarketPrice"),
            meta.get("chartPreviousClose") or meta.get("previousClose"),
        )
    except Exception:
        return None, None


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"=== fetch_daily.py (asian-indexes)  {ts} ===")
    now = datetime.now(timezone.utc)
    indices = []

    stooq_start = (now - timedelta(days=6 * 365)).strftime("%Y%m%d")
    stooq_end   = now.strftime("%Y%m%d")

    for name, (stooq_sym, yahoo_sym) in INDEXES.items():
        history = None

        if stooq_sym:
            print(f"  {name} (Stooq {stooq_sym})")
            try:
                r = requests.get(
                    STOOQ_BASE,
                    params={"s": stooq_sym, "d1": stooq_start, "d2": stooq_end, "i": "d"},
                    headers=HEADERS, timeout=20,
                )
                r.raise_for_status()
                parsed = parse_stooq_csv(r.text)
                if len(parsed) >= 2:
                    history = parsed
            except Exception as e:
                print(f"    Stooq failed for {name}: {e}")

        if not history:
            print(f"  {name} (Yahoo {yahoo_sym})")
            try:
                r = requests.get(
                    f"{YAHOO_BASE}{yahoo_sym}",
                    params={"interval": "1d", "range": "5y"},
                    headers=HEADERS, timeout=20,
                )
                r.raise_for_status()
                result = r.json()["chart"]["result"][0]
                timestamps = result["timestamp"]
                closes = result["indicators"]["quote"][0]["close"]
                parsed = [
                    {"x": int(ts) * 1000, "y": round(float(c), 4)}
                    for ts, c in zip(timestamps, closes)
                    if c is not None
                ]
                if len(parsed) >= 2:
                    history = parsed
            except Exception as e:
                print(f"    Yahoo failed for {name}: {e}")

        if not history:
            print(f"    SKIP {name}: no data from Stooq or Yahoo")
            time.sleep(0.4)
            continue

        price, prev_close = fetch_live_yahoo(yahoo_sym)
        if not price:
            price = history[-1]["y"]
            prev_close = history[-2]["y"]
        change_abs = price - prev_close
        change_pct = change_abs / prev_close * 100.0
        indices.append({
            "name": name, "price": price,
            "changePct": change_pct, "changeAbs": change_abs,
            "history": history,
        })
        time.sleep(0.4)

    if len(indices) < 3:
        print(f"  Only {len(indices)} index/indices fetched (expected 6) — skipping save to preserve existing data.")
        return

    save("data/asian-history.json", {
        "fetched_at": int(time.time() * 1000),
        "indices": indices,
    })
    print("=== Done ===")


if __name__ == "__main__":
    main()
