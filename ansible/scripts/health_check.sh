#!/usr/bin/env bash
# ============================================================
# Kafka Cluster Health Check Script
# Verifies that Kafka is ACTUALLY WORKING, not just running
# ============================================================
set -euo pipefail

KAFKA_HOST="${1:-localhost}"
KAFKA_PORT="${2:-9092}"
CONTAINER_NAME="${3:-kafka-broker}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0

check() {
    local name="$1"
    local result="$2"
    if [ "$result" = "0" ]; then
        echo -e "  ${GREEN}PASS${NC}: $name"
        PASS=$((PASS+1))
    elif [ "$result" = "1" ]; then
        echo -e "  ${RED}FAIL${NC}: $name"
        FAIL=$((FAIL+1))
    else
        echo -e "  ${YELLOW}WARN${NC}: $name"
        WARN=$((WARN+1))
    fi
}

echo "============================================================"
echo "Kafka Cluster Health Check"
echo "Target: ${KAFKA_HOST}:${KAFKA_PORT}"
echo "============================================================"

echo ""
echo "--- Container Check ---"

docker inspect "${CONTAINER_NAME}" > /dev/null 2>&1
check "Container exists" "$?"

CONTAINER_STATUS=$(docker inspect --format='{{.State.Status}}' "${CONTAINER_NAME}" 2>/dev/null || echo "not_found")
[ "${CONTAINER_STATUS}" = "running" ]
check "Container is running (status=${CONTAINER_STATUS})" "$?"

echo ""
echo "--- Network Check ---"

timeout 5 bash -c "echo > /dev/tcp/${KAFKA_HOST}/${KAFKA_PORT}" 2>/dev/null
check "Client port ${KAFKA_PORT} is reachable" "$?"

echo ""
echo "--- Kafka Broker Check ---"

docker exec "${CONTAINER_NAME}" /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "localhost:${KAFKA_PORT}" > /dev/null 2>&1
check "Broker API versions available" "$?"

echo ""
echo "--- Topic Check ---"

TOPICS=$(docker exec "${CONTAINER_NAME}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "localhost:${KAFKA_PORT}" --list 2>/dev/null || echo "")

echo "${TOPICS}" | grep -q "analytics.events"
check "Topic 'analytics.events' exists" "$?"

if echo "${TOPICS}" | grep -q "analytics.events"; then
    TOPIC_DESC=$(docker exec "${CONTAINER_NAME}" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "localhost:${KAFKA_PORT}" \
        --describe --topic analytics.events 2>/dev/null || echo "")

    echo "${TOPIC_DESC}" | grep -q "Leader: [0-9]"
    check "Topic has partition leaders" "$?"

    ISR_COUNT=$(echo "${TOPIC_DESC}" | grep -c "Isr:" || echo "0")
    [ "${ISR_COUNT}" -gt 0 ]
    check "Topic has ISR information (${ISR_COUNT} partitions)" "$?"
fi

echo ""
echo "--- Consumer Group Check ---"

docker exec "${CONTAINER_NAME}" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server "localhost:${KAFKA_PORT}" --list > /dev/null 2>&1
check "Consumer groups accessible" "$?"

echo ""
echo "--- Data Integrity Check ---"

# Produce and consume a test message
TEST_MSG="health_check_$(date +%s)"
echo "${TEST_MSG}" | docker exec -i "${CONTAINER_NAME}" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "localhost:${KAFKA_PORT}" \
    --topic analytics.events 2>/dev/null

RECEIVED=$(timeout 10 docker exec "${CONTAINER_NAME}" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "localhost:${KAFKA_PORT}" \
    --topic analytics.events \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null | grep -c "${TEST_MSG}" || echo "0")

[ "${RECEIVED}" -ge 1 ]
check "Produce/consume roundtrip works" "$?"

echo ""
echo "============================================================"
echo "Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}, ${YELLOW}${WARN} warnings${NC}"
echo "============================================================"

if [ "${FAIL}" -gt 0 ]; then
    exit 1
fi
exit 0
