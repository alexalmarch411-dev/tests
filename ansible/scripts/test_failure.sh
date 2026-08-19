#!/usr/bin/env bash
# ============================================================
# Failure / Recovery Test Script
# Tests Kafka resilience when a broker goes down
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANSIBLE_DIR="$(dirname "${SCRIPT_DIR}")"
INVENTORY="${1:-inventories/yandex/hosts.yml}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "============================================================"
echo "Kafka Failure / Recovery Test"
echo "============================================================"

cd "${ANSIBLE_DIR}"

# Get hosts
# ansible_host = публичный IP для SSH, kafka_internal_ip = внутренний IP для Kafka-трафика
KAFKA_1_HOST=$(ansible-inventory -i "${INVENTORY}" --host kafka-1 | jq -r '.ansible_host // .ansible_ssh_host')
KAFKA_2_HOST=$(ansible-inventory -i "${INVENTORY}" --host kafka-2 | jq -r '.ansible_host // .ansible_ssh_host')
KAFKA_3_HOST=$(ansible-inventory -i "${INVENTORY}" --host kafka-3 | jq -r '.ansible_host // .ansible_ssh_host')

KAFKA_1_INTERNAL=$(ansible-inventory -i "${INVENTORY}" --host kafka-1 | jq -r '.kafka_internal_ip')
KAFKA_2_INTERNAL=$(ansible-inventory -i "${INVENTORY}" --host kafka-2 | jq -r '.kafka_internal_ip')
KAFKA_3_INTERNAL=$(ansible-inventory -i "${INVENTORY}" --host kafka-3 | jq -r '.kafka_internal_ip')

# Внутренние IP — адреса, которые рекламирует Kafka (advertised.listeners)
BROKERS="${KAFKA_1_INTERNAL}:9092,${KAFKA_2_INTERNAL}:9092,${KAFKA_3_INTERNAL}:9092"

echo "Brokers: ${BROKERS}"

# Step 1: Verify all brokers are up
echo ""
echo "--- Step 1: Verify All Brokers Running ---"
for HOST in "${KAFKA_1_HOST}" "${KAFKA_2_HOST}" "${KAFKA_3_HOST}"; do
    STATUS=$(ssh "ubuntu@${HOST}" "docker inspect --format='{{.State.Status}}' kafka-broker 2>/dev/null || echo 'not_found'")
    echo "  ${HOST}: ${STATUS}"
done

# Step 2: Create test topic
echo ""
echo "--- Step 2: Create Test Topic ---"
ssh "ubuntu@${KAFKA_1_HOST}" "docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic failure-test \
    --partitions 3 --replication-factor 3 \
    --if-not-exists" 2>/dev/null || true

# Step 3: Produce messages
echo ""
echo "--- Step 3: Produce Messages ---"
ssh "ubuntu@${KAFKA_1_HOST}" 'for i in $(seq 1 500); do echo "{\"event_id\":\"fail-test-'$i'\",\"user_id\":\"user-1\",\"event_type\":\"page_view\",\"timestamp\":\"2026-01-01T00:00:00Z\",\"page\":\"/test\",\"session_id\":\"s-1\"}"; done | docker exec -i kafka-broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic failure-test' 2>/dev/null
echo "  Produced 500 messages to failure-test"

# Step 4: Stop one broker
echo ""
echo "--- Step 4: Stopping Broker kafka-3 (${KAFKA_3_HOST}) ---"
ssh "ubuntu@${KAFKA_3_HOST}" "docker stop kafka-broker"
echo "  Broker kafka-3 stopped"
sleep 10

# Step 5: Verify cluster still works
echo ""
echo "--- Step 5: Verify Cluster with 2 Brokers ---"
ssh "ubuntu@${KAFKA_1_HOST}" "docker exec kafka-broker /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092" > /dev/null 2>&1
echo "  Cluster still accessible: OK"

# Step 6: Continue producing with 1 broker down
echo ""
echo "--- Step 6: Produce Messages with Broker Down ---"
ssh "ubuntu@${KAFKA_1_HOST}" 'for i in $(seq 501 1000); do echo "{\"event_id\":\"fail-test-'$i'\",\"user_id\":\"user-1\",\"event_type\":\"page_view\",\"timestamp\":\"2026-01-01T00:00:00Z\",\"page\":\"/test\",\"session_id\":\"s-1\"}"; done | docker exec -i kafka-broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic failure-test' 2>/dev/null
echo "  Produced 500 more messages (total 1000)"

# Step 7: Consume and verify
echo ""
echo "--- Step 7: Verify Messages ---"
MSG_COUNT=$(ssh "ubuntu@${KAFKA_1_HOST}" "timeout 15 docker exec kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic failure-test \
    --from-beginning \
    --max-messages 10000 \
    --timeout-ms 10000 2>/dev/null | wc -l" || echo "0")
echo "  Messages consumed: ${MSG_COUNT}"

# Step 8: Restart broker
echo ""
echo "--- Step 8: Restarting Broker kafka-3 ---"
ssh "ubuntu@${KAFKA_3_HOST}" "docker start kafka-broker"
echo "  Broker kafka-3 restarted"
sleep 30

# Step 9: Verify ISR recovery
echo ""
echo "--- Step 9: Verify ISR Recovery ---"
TOPIC_DESC=$(ssh "ubuntu@${KAFKA_1_HOST}" "docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic failure-test 2>/dev/null" || echo "")
echo "${TOPIC_DESC}"

# Check ISR
UNDER_REPLICATED=$(echo "${TOPIC_DESC}" | grep -c "Isr: [^[]*[0-9]$" || echo "0")
echo "  Under-replicated partitions: ${UNDER_REPLICATED}"

# Step 10: Verify data integrity
echo ""
echo "--- Step 10: Verify Data After Recovery ---"
MSG_COUNT_AFTER=$(ssh "ubuntu@${KAFKA_1_HOST}" "timeout 15 docker exec kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic failure-test \
    --from-beginning \
    --max-messages 10000 \
    --timeout-ms 10000 2>/dev/null | wc -l" || echo "0")
echo "  Messages after recovery: ${MSG_COUNT_AFTER}"

# Summary
echo ""
echo "============================================================"
echo "FAILURE TEST RESULTS"
echo "============================================================"
echo "  Messages produced:          1000"
echo "  Messages consumed (2/3):    ${MSG_COUNT}"
echo "  Messages consumed (3/3):    ${MSG_COUNT_AFTER}"
echo ""
echo "  Analysis:"
echo "  - Loss of 1 broker (RF=3, min.insync.replicas=2):"
echo "    Writes continue because 2 replicas are still in-sync."
echo "    No data loss expected."
echo "  - Loss of 2 brokers:"
echo "    Writes would BLOCK because min.insync.replicas=2"
echo "    cannot be satisfied with only 1 broker."
echo "    This is BY DESIGN to prevent data loss."
echo "  - RF=3 ensures data survives 1 broker failure."
echo "  - min.insync.replicas=2 ensures write durability."
echo "============================================================"
