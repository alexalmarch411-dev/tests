#!/usr/bin/env python3
"""
End-to-end test: Kafka -> ClickHouse pipeline verification.

Usage:
  python3 scripts/test_e2e.py \
    --kafka-brokers kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    --clickhouse-host clickhouse-1 \
    --clickhouse-port 8123 \
    --count 10000
"""

import argparse
import json
import sys
import time
import subprocess
import uuid
import random
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient
except ImportError:
    print("ERROR: kafka-python not installed. Run: pip install kafka-python")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests")
    sys.exit(1)

EVENT_TYPES = ["page_view", "login", "logout", "button_click", "purchase"]
PAGES = ["/", "/pricing", "/features", "/docs", "/blog"]
USER_IDS = [f"user-{i}" for i in range(1, 51)]


def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "page": random.choice(PAGES),
        "session_id": f"session-{uuid.uuid4().hex[:8]}",
    }


def clickhouse_query(host, port, query):
    """Execute a query against ClickHouse via HTTP."""
    url = f"http://{host}:{port}/"
    resp = requests.post(url, data=query, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"ClickHouse error: {resp.status_code} {resp.text}")
    return resp.text.strip()


def check_kafka_cluster(brokers):
    """Verify Kafka cluster health."""
    print("\n=== Kafka Cluster Health ===")
    admin = KafkaAdminClient(bootstrap_servers=brokers)

    cluster_meta = admin.describe_cluster()
    print(f"  Cluster ID: {cluster_meta.get('cluster_id', 'N/A')}")
    brokers_list = cluster_meta.get('brokers', [])
    print(f"  Brokers: {len(brokers_list)}")
    for b in brokers_list:
        print(f"    - Node {b.get('node_id')}: {b.get('host')}:{b.get('port')}")

    topics = admin.list_topics()
    print(f"  Topics: {len(topics)}")
    if 'analytics.events' in topics:
        topic_meta = admin.describe_topics(['analytics.events'])
        for t in topic_meta:
            if t['topic'] == 'analytics.events':
                partitions = t['partitions']
                print(f"  analytics.events: {len(partitions)} partitions")
                for p in partitions:
                    print(f"    Partition {p['partition']}: leader={p['leader']}, ISR={p['isr']}")

    admin.close()
    return len(brokers_list)


def produce_events(brokers, topic, count):
    """Produce events to Kafka."""
    print(f"\n=== Producing {count} events ===")
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    produced = 0
    start = time.time()
    for i in range(count):
        event = generate_event()
        future = producer.send(topic, value=event)
        future.get(timeout=10)
        produced += 1
        if produced % 2000 == 0:
            print(f"  Produced: {produced}/{count}")

    producer.flush()
    producer.close()
    elapsed = time.time() - start
    print(f"  Produced {produced} events in {elapsed:.2f}s")
    return produced


def verify_kafka_messages(brokers, topic, expected_count):
    """Verify messages exist in Kafka by counting them."""
    print(f"\n=== Verifying Kafka messages ===")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f'verify-{uuid.uuid4().hex[:8]}',
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    count = 0
    for message in consumer:
        count += 1
        if count % 2000 == 0:
            print(f"  Consumed: {count}")

    consumer.close()
    print(f"  Kafka messages found: {count}")
    return count


def verify_clickhouse_data(host, port, expected_count):
    """Verify data in ClickHouse."""
    print(f"\n=== Verifying ClickHouse data ===")

    # Wait for data to be flushed
    print("  Waiting for ClickHouse to process data...")
    for attempt in range(30):
        count_str = clickhouse_query(host, port,
            "SELECT count() FROM analytics.events")
        count = int(count_str) if count_str.isdigit() else 0
        if count >= expected_count:
            break
        print(f"  Attempt {attempt+1}/30: {count}/{expected_count} rows")
        time.sleep(2)

    print(f"  ClickHouse rows: {count}")

    if count > 0:
        sample = clickhouse_query(host, port,
            "SELECT event_id, user_id, event_type, timestamp, page FROM analytics.events ORDER BY timestamp DESC LIMIT 5 FORMAT Pretty")
        print(f"\n  Sample data:\n{sample}")

        stats = clickhouse_query(host, port,
            "SELECT event_type, count() as cnt FROM analytics.events GROUP BY event_type ORDER BY cnt DESC FORMAT Pretty")
        print(f"\n  Event type distribution:\n{stats}")

    return count


def main():
    parser = argparse.ArgumentParser(description="End-to-end Kafka -> ClickHouse test")
    parser.add_argument("--kafka-brokers", required=True)
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--topic", default="analytics.events")
    parser.add_argument("--count", type=int, default=10000)
    args = parser.parse_args()

    print("=" * 60)
    print("End-to-End Test: Kafka -> ClickHouse")
    print("=" * 60)

    # Step 1: Check Kafka cluster
    broker_count = check_kafka_cluster(args.kafka_brokers)
    if broker_count < 2:
        print(f"WARNING: Only {broker_count} brokers available")

    # Step 2: Produce events
    produced = produce_events(args.kafka_brokers, args.topic, args.count)

    # Step 3: Verify in Kafka
    kafka_count = verify_kafka_messages(args.kafka_brokers, args.topic, produced)

    # Step 4: Verify in ClickHouse
    ch_count = verify_clickhouse_data(args.clickhouse_host, args.clickhouse_port, produced)

    # Summary
    print("\n" + "=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    print(f"  Produced:          {produced}")
    print(f"  Kafka consumed:    {kafka_count}")
    print(f"  ClickHouse rows:   {ch_count}")
    print(f"  Lost (Kafka):      {produced - kafka_count}")
    print(f"  Lost (ClickHouse): {produced - ch_count}")

    if produced == kafka_count == ch_count:
        print("\n  STATUS: PASS - All events delivered end-to-end!")
        return 0
    else:
        print("\n  STATUS: FAIL - Data loss detected!")
        if produced != kafka_count:
            print(f"  WARNING: Kafka consumed {kafka_count} but produced {produced}")
        if kafka_count != ch_count:
            print(f"  WARNING: ClickHouse has {ch_count} but Kafka had {kafka_count}")
            print("  TIP: ClickHouse Kafka engine may need more time to flush.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
