#!/usr/bin/env python3
"""
Event generator for a data engineering portfolio.

Outputs newline-delimited JSON (NDJSON) for clickstream-style events with:
- Realistic session behavior and event sequences
- Optional invalid/bad records (for quarantine testing later)
- Deterministic runs via seed
- Partition-friendly timestamps (dt/hr can be derived downstream)

Usage examples:
  python ingestion/generate_events.py --out data/raw/events.ndjson --rows 5000
  python ingestion/generate_events.py --out data/raw/events.ndjson --rows 20000 --bad-rate 0.01 --seed 42
  python ingestion/generate_events.py --out data/raw/events.ndjson --minutes 10 --rate 200
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


EVENT_TYPES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout_start",
    "purchase",
    "login",
]

PLATFORMS = ["web", "ios", "android"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
COUNTRIES = ["US", "CA", "UK", "AU", "DE", "FR", "BR", "IN", "JP"]
MARKETING_SOURCES = ["organic", "email", "paid_social", "paid_search", "referral", "direct", None]

CATEGORIES = ["electronics", "home", "toys", "fashion", "sports", "beauty", "grocery"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_z(dt: datetime) -> str:
    # Standard ISO-8601 with Z suffix.
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def weighted_choice(rng: random.Random, items: List[Tuple[Any, float]]) -> Any:
    values, weights = zip(*items)
    return rng.choices(values, weights=weights, k=1)[0]


def random_user_agent(rng: random.Random, platform: str) -> str:
    if platform == "web":
        return weighted_choice(
            rng,
            [
                ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0", 0.55),
                ("Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 Safari/17.0", 0.25),
                ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/123.0", 0.20),
            ],
        )
    if platform == "ios":
        return "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile"
    return "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 Chrome/124.0 Mobile"


@dataclass
class Event:
    event_id: str
    event_time: str
    ingest_time: str
    event_type: str
    user_id: str
    session_id: str
    platform: str
    device_type: str
    ip_country: str
    user_agent: str
    page_url: str
    referrer_url: Optional[str]
    marketing_source: Optional[str]
    marketing_campaign: Optional[str]
    product_id: Optional[str]
    category: Optional[str]
    price: Optional[float]
    currency: Optional[str]
    quantity: Optional[int]
    order_id: Optional[str]


def build_product_catalog(rng: random.Random, n_products: int = 250) -> pd.DataFrame:
    product_ids = [f"P{str(i).zfill(5)}" for i in range(1, n_products + 1)]
    categories = rng.choices(CATEGORIES, k=n_products)

    # Category-based price bands (simple realism)
    base_prices = {
        "electronics": (49, 899),
        "home": (9, 199),
        "toys": (5, 149),
        "fashion": (10, 299),
        "sports": (8, 399),
        "beauty": (6, 129),
        "grocery": (2, 49),
    }

    prices = []
    for cat in categories:
        lo, hi = base_prices[cat]
        prices.append(round(rng.uniform(lo, hi), 2))

    df = pd.DataFrame(
        {
            "product_id": product_ids,
            "category": categories,
            "price": prices,
            "currency": ["USD"] * n_products,
        }
    )
    return df


def simulate_session_events(
    rng: random.Random,
    user_id: str,
    session_id: str,
    start_time: datetime,
    platform: str,
    device_type: str,
    ip_country: str,
    user_agent: str,
    marketing_source: Optional[str],
    marketing_campaign: Optional[str],
    catalog: pd.DataFrame,
    bad_rate: float,
) -> List[Dict[str, Any]]:
    """
    Create a plausible event sequence.
    Typical patterns:
      - page_view(s)
      - optional login
      - product_view(s)
      - optional add_to_cart
      - optional checkout_start
      - optional purchase
    """
    events: List[Event] = []

    t = start_time
    ingest_time = utc_now()

    def add_event(
        event_type: str,
        page_url: str,
        referrer_url: Optional[str],
        product_row: Optional[pd.Series] = None,
        quantity: Optional[int] = None,
        order_id: Optional[str] = None,
    ) -> None:
        nonlocal t
        # Advance time slightly between events
        t = t + timedelta(seconds=rng.randint(2, 35))

        product_id = None
        category = None
        price = None
        currency = None
        if product_row is not None:
            product_id = str(product_row["product_id"])
            category = str(product_row["category"])
            price = float(product_row["price"])
            currency = str(product_row["currency"])

        ev = Event(
            event_id=str(uuid.uuid4()),
            event_time=isoformat_z(t),
            ingest_time=isoformat_z(ingest_time),
            event_type=event_type,
            user_id=user_id,
            session_id=session_id,
            platform=platform,
            device_type=device_type,
            ip_country=ip_country,
            user_agent=user_agent,
            page_url=page_url,
            referrer_url=referrer_url,
            marketing_source=marketing_source,
            marketing_campaign=marketing_campaign,
            product_id=product_id,
            category=category,
            price=price,
            currency=currency,
            quantity=quantity,
            order_id=order_id,
        )
        events.append(ev)

    # Basic browsing: 1–4 page views
    n_page_views = rng.randint(1, 4)
    ref = weighted_choice(rng, [(None, 0.60), ("https://www.google.com", 0.25), ("https://www.facebook.com", 0.10), ("https://news.ycombinator.com", 0.05)])
    for i in range(n_page_views):
        add_event(
            "page_view",
            page_url=weighted_choice(
                rng,
                [
                    ("https://example.com/", 0.35),
                    ("https://example.com/search", 0.30),
                    ("https://example.com/category", 0.20),
                    ("https://example.com/deals", 0.15),
                ],
            ),
            referrer_url=ref if i == 0 else "https://example.com/",
        )

    # Optional login (more common on mobile apps)
    if rng.random() < (0.22 if platform != "web" else 0.12):
        add_event("login", page_url="https://example.com/login", referrer_url="https://example.com/")

    # Product browsing: 1–6 product views
    n_product_views = rng.randint(1, 6)
    viewed_products = catalog.sample(n=n_product_views, replace=False, random_state=rng.randint(1, 10_000))
    for _, row in viewed_products.iterrows():
        add_event(
            "product_view",
            page_url=f"https://example.com/product/{row['product_id']}",
            referrer_url="https://example.com/search",
            product_row=row,
        )

    # Conversion decisions
    will_add_to_cart = rng.random() < 0.35
    will_checkout = will_add_to_cart and (rng.random() < 0.55)
    will_purchase = will_checkout and (rng.random() < 0.70)

    cart_product = viewed_products.sample(n=1, random_state=rng.randint(1, 10_000)).iloc[0]
    qty = rng.randint(1, 3)

    if will_add_to_cart:
        add_event(
            "add_to_cart",
            page_url="https://example.com/cart",
            referrer_url=f"https://example.com/product/{cart_product['product_id']}",
            product_row=cart_product,
            quantity=qty,
        )

    order_id = None
    if will_checkout:
        add_event(
            "checkout_start",
            page_url="https://example.com/checkout",
            referrer_url="https://example.com/cart",
            product_row=cart_product,
            quantity=qty,
        )

    if will_purchase:
        order_id = f"O{uuid.uuid4().hex[:12].upper()}"
        add_event(
            "purchase",
            page_url="https://example.com/confirmation",
            referrer_url="https://example.com/checkout",
            product_row=cart_product,
            quantity=qty,
            order_id=order_id,
        )

    # Convert dataclasses to dicts
    records: List[Dict[str, Any]] = [asdict(e) for e in events]

    # Inject some bad records for later quarantine testing
    # Types of badness:
    # - missing user_id
    # - invalid event_type
    # - purchase missing order_id
    for r in records:
        if bad_rate > 0 and rng.random() < bad_rate:
            bad_kind = rng.choice(["missing_user_id", "invalid_event_type", "purchase_missing_order_id"])
            if bad_kind == "missing_user_id":
                r["user_id"] = ""  # violates not-null expectation
            elif bad_kind == "invalid_event_type":
                r["event_type"] = "unknown_event"
            elif bad_kind == "purchase_missing_order_id" and r["event_type"] == "purchase":
                r["order_id"] = None

    return records


def generate_events(
    seed: int,
    total_rows: Optional[int],
    minutes: Optional[int],
    rate: int,
    bad_rate: float,
) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    catalog = build_product_catalog(rng)

    records: List[Dict[str, Any]] = []

    # We simulate multiple users/sessions rather than generating independent events.
    # Each session yields a variable number of events.
    target_rows = total_rows if total_rows is not None else (minutes * rate * 60 if minutes is not None else 10_000)

    base_time = utc_now() - timedelta(minutes=5)  # slight backshift for realism

    while len(records) < target_rows:
        user_id = f"U{rng.randint(1, 50_000):06d}"
        session_id = f"S{uuid.uuid4().hex[:16].upper()}"

        platform = rng.choice(PLATFORMS)
        device_type = weighted_choice(rng, [("mobile", 0.55), ("desktop", 0.35), ("tablet", 0.10)]) if platform == "web" else weighted_choice(rng, [("mobile", 0.80), ("tablet", 0.20)])
        ip_country = rng.choice(COUNTRIES)
        user_agent = random_user_agent(rng, platform)

        marketing_source = weighted_choice(rng, [(s, 1.0) for s in MARKETING_SOURCES])
        marketing_campaign = None
        if marketing_source in ("email", "paid_social", "paid_search"):
            marketing_campaign = weighted_choice(
                rng,
                [
                    ("spring_sale", 0.35),
                    ("clearance", 0.25),
                    ("new_arrivals", 0.20),
                    ("brand_awareness", 0.20),
                ],
            )

        # Stagger sessions over time
        start_time = base_time + timedelta(seconds=rng.randint(0, 300))

        session_records = simulate_session_events(
            rng=rng,
            user_id=user_id,
            session_id=session_id,
            start_time=start_time,
            platform=platform,
            device_type=device_type,
            ip_country=ip_country,
            user_agent=user_agent,
            marketing_source=marketing_source,
            marketing_campaign=marketing_campaign,
            catalog=catalog,
            bad_rate=bad_rate,
        )
        records.extend(session_records)

    # Trim to exact target
    return records[:target_rows]


def write_ndjson(records: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic clickstream NDJSON events.")
    p.add_argument("--out", required=True, help="Output file path, e.g. data/raw/events.ndjson")
    grp = p.add_mutually_exclusive_group(required=False)
    grp.add_argument("--rows", type=int, help="Total number of event records to generate.")
    grp.add_argument("--minutes", type=int, help="Generate events for N minutes at given --rate (events/sec).")
    p.add_argument("--rate", type=int, default=50, help="Events/sec used with --minutes (default: 50).")
    p.add_argument("--bad-rate", type=float, default=0.0, help="Fraction of records to intentionally corrupt (0.0–0.05 recommended).")
    p.add_argument("--seed", type=int, default=7, help="Random seed for deterministic output.")
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    if args.bad_rate < 0 or args.bad_rate > 0.20:
        print("ERROR: --bad-rate should be between 0.0 and 0.20", file=sys.stderr)
        return 2

    records = generate_events(
        seed=args.seed,
        total_rows=args.rows,
        minutes=args.minutes,
        rate=args.rate,
        bad_rate=args.bad_rate,
    )
    out_path = Path(args.out)
    write_ndjson(records, out_path)

    # Quick summary
    df = pd.DataFrame(records)
    summary = {
        "rows": len(df),
        "unique_users": int(df["user_id"].nunique(dropna=False)),
        "unique_sessions": int(df["session_id"].nunique(dropna=False)),
        "event_type_counts": df["event_type"].value_counts(dropna=False).to_dict(),
        "min_event_time": df["event_time"].min(),
        "max_event_time": df["event_time"].max(),
        "out": str(out_path),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
