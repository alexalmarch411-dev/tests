#!/usr/bin/env python3
"""
Test Event Producer for Kafka → ClickHouse pipeline.
Generates synthetic analytics events and produces them to Kafka.

Usage:
  python3 scripts/test_producer.py --brokers kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    --topic analytics.events --count 10000
"""

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
except ImportError:
    print("ERROR: kafka-python not installed. Run: pip install kafka-python")
    sys.exit(1)

EVENT_TYPES = ["page_view", "login", "logout", "button_click", "purchase"]
PAGES = ["/", "/pricing", "/features", "/docs", "/blog", "/about", "/contact", "/signup", "/login", "/dashboard"]
USER_IDS = [f"user-{i}" for i in range(1, 101)]
SESSION_IDS = [f"session-{uuid.uuid4().hex[:8]}" for _ in range(50)]


def generate_event():
    """Generate a single synthetic analytics event."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "page": random.choice(PAGES),
        "session_id": random.choice(SESSION_IDS),
    }


def main():
    parser = argparse.ArgumentParser(description="Kafka Test Event Producer")
    parser.add_argument("--brokers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="analytics.events", help="Target topic")
    parser.add_argument("--count", type=int, default=10000, help="Number of events")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size")
    parser.add_argument("--rate", type=float, default=0, help="Events per second (0=unlimited)")
    args = parser.parse_args()

    print(f"Connecting to Kafka: {args.brokers}")
    producer = KafkaProducer(
        bootstrap_servers=args.brokers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
        batch_size=16384,
        linger_ms=10,
    )

    print(f"Producing {args.count} events to '{args.topic}'...")
    produced = 0
    start_time = time.time()
    batch_start = start_time

    try:
        for i in range(args.count):
            event = generate_event()
            future = producer.send(args.topic, value=event)
            future.get(timeout=10)
            produced += 1

            if args.rate > 0:
                elapsed = time.time() - batch_start
                expected = (i + 1) / args.rate
                if elapsed < expected:
                    time.sleep(expected - elapsed)

            if produced % 1000 == 0:
                elapsed = time.time() - start_time
                rate = produced / elapsed if elapsed > 0 else 0
                print(f"  Produced: {produced}/{args.count} ({rate:.0f} events/sec)")

    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    finally:
        producer.flush()
        producer.close()

    elapsed = time.time() - start_time
    print(f"\nDone! Produced {produced} events in {elapsed:.2f}s ({produced/elapsed:.0f} events/sec)")
    print(f"Topic: {args.topic}")


if __name__ == "__main__":
    main()
