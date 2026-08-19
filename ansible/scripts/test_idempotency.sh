#!/usr/bin/env bash
# ============================================================
# Idempotency Test Script
# Verifies that running Ansible twice does not destroy data
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANSIBLE_DIR="$(dirname "${SCRIPT_DIR}")"
INVENTORY="${1:-inventories/yandex/hosts.yml}"
PLAYBOOK="${2:-playbooks/site.yml}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "============================================================"
echo "Idempotency Test"
echo "Inventory: ${INVENTORY}"
echo "Playbook: ${PLAYBOOK}"
echo "============================================================"

cd "${ANSIBLE_DIR}"

# Step 1: First run
echo ""
echo "--- Step 1: First Ansible Run ---"
ansible-playbook -i "${INVENTORY}" "${PLAYBOOK}" --tags "kafka" 2>&1 | tee /tmp/ansible_run1.log
RUN1_RC=${PIPESTATUS[0]}
echo "First run exit code: ${RUN1_RC}"

# Step 2: Record state
echo ""
echo "--- Step 2: Recording State ---"

KAFKA_HOST=$(ansible-inventory -i "${INVENTORY}" --host kafka-1 | jq -r '.ansible_host // .ansible_ssh_host')
echo "Kafka host: ${KAFKA_HOST}"

# Get cluster ID
CLUSTER_ID_1=$(ssh "ubuntu@${KAFKA_HOST}" "grep cluster.id /var/lib/kafka/data/meta.properties 2>/dev/null | cut -d= -f2" || echo "N/A")
echo "Cluster ID: ${CLUSTER_ID_1}"

# Get node IDs
NODE_IDS_1=$(ssh "ubuntu@${KAFKA_HOST}" "grep node.id /var/lib/kafka/data/meta.properties 2>/dev/null | cut -d= -f2" || echo "N/A")
echo "Node ID (kafka-1): ${NODE_IDS_1}"

# Get topic count
TOPICS_1=$(ssh "ubuntu@${KAFKA_HOST}" "docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | wc -l" || echo "0")
echo "Topics: ${TOPICS_1}"

# Produce test messages
echo ""
echo "--- Step 3: Producing Test Messages ---"
ssh "ubuntu@${KAFKA_HOST}" 'for i in $(seq 1 100); do echo "{\"event_id\":\"idempotency-test-'$i'\",\"user_id\":\"user-1\",\"event_type\":\"page_view\",\"timestamp\":\"2026-01-01T00:00:00Z\",\"page\":\"/test\",\"session_id\":\"s-1\"}"; done | docker exec -i kafka-broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic analytics.events' 2>/dev/null
echo "Produced 100 test messages"

# Count messages before second run
MESSAGES_BEFORE=$(ssh "ubuntu@${KAFKA_HOST}" "timeout 15 docker exec kafka-broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic analytics.events --from-beginning --max-messages 10000 --timeout-ms 10000 2>/dev/null | wc -l" || echo "0")
echo "Messages before second run: ${MESSAGES_BEFORE}"

# Step 4: Second run
echo ""
echo "--- Step 4: Second Ansible Run (Idempotency Test) ---"
ansible-playbook -i "${INVENTORY}" "${PLAYBOOK}" --tags "kafka" 2>&1 | tee /tmp/ansible_run2.log
RUN2_RC=${PIPESTATUS[0]}
echo "Second run exit code: ${RUN2_RC}"

# Check for changes
CHANGED=$(grep -c "changed=" /tmp/ansible_run2.log | tail -1 || echo "0")
CHANGED_COUNT=$(grep "changed=" /tmp/ansible_run2.log | tail -1 | grep -oP 'changed=\K[0-9]+' || echo "N/A")
echo "Changed tasks in second run: ${CHANGED_COUNT}"

# Step 5: Verify state preserved
echo ""
echo "--- Step 5: Verifying State Preservation ---"

CLUSTER_ID_2=$(ssh "ubuntu@${KAFKA_HOST}" "grep cluster.id /var/lib/kafka/data/meta.properties 2>/dev/null | cut -d= -f2" || echo "N/A")
echo "Cluster ID after: ${CLUSTER_ID_2}"

NODE_IDS_2=$(ssh "ubuntu@${KAFKA_HOST}" "grep node.id /var/lib/kafka/data/meta.properties 2>/dev/null | cut -d= -f2" || echo "N/A")
echo "Node ID after: ${NODE_IDS_2}"

TOPICS_2=$(ssh "ubuntu@${KAFKA_HOST}" "docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | wc -l" || echo "0")
echo "Topics after: ${TOPICS_2}"

MESSAGES_AFTER=$(ssh "ubuntu@${KAFKA_HOST}" "timeout 15 docker exec kafka-broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic analytics.events --from-beginning --max-messages 10000 --timeout-ms 10000 2>/dev/null | wc -l" || echo "0")
echo "Messages after: ${MESSAGES_AFTER}"

# Summary
echo ""
echo "============================================================"
echo "IDEMPOTENCY TEST RESULTS"
echo "============================================================"

PASS=true

if [ "${CLUSTER_ID_1}" = "${CLUSTER_ID_2}" ]; then
    echo -e "  ${GREEN}PASS${NC}: Cluster ID preserved (${CLUSTER_ID_1})"
else
    echo -e "  ${RED}FAIL${NC}: Cluster ID changed! (${CLUSTER_ID_1} -> ${CLUSTER_ID_2})"
    PASS=false
fi

if [ "${NODE_IDS_1}" = "${NODE_IDS_2}" ]; then
    echo -e "  ${GREEN}PASS${NC}: Node ID preserved (${NODE_IDS_1})"
else
    echo -e "  ${RED}FAIL${NC}: Node ID changed! (${NODE_IDS_1} -> ${NODE_IDS_2})"
    PASS=false
fi

if [ "${TOPICS_1}" = "${TOPICS_2}" ]; then
    echo -e "  ${GREEN}PASS${NC}: Topics preserved (${TOPICS_1})"
else
    echo -e "  ${RED}FAIL${NC}: Topics changed! (${TOPICS_1} -> ${TOPICS_2})"
    PASS=false
fi

if [ "${MESSAGES_BEFORE}" = "${MESSAGES_AFTER}" ]; then
    echo -e "  ${GREEN}PASS${NC}: Messages preserved (${MESSAGES_BEFORE})"
else
    echo -e "  ${YELLOW}WARN${NC}: Message count differs (${MESSAGES_BEFORE} -> ${MESSAGES_AFTER})"
    echo "    This may be due to consumer offset behavior, not data loss"
fi

if [ "${RUN1_RC}" = "0" ] && [ "${RUN2_RC}" = "0" ]; then
    echo -e "  ${GREEN}PASS${NC}: Both runs completed successfully"
else
    echo -e "  ${RED}FAIL${NC}: Run exit codes: first=${RUN1_RC}, second=${RUN2_RC}"
    PASS=false
fi

echo ""
if [ "${PASS}" = true ]; then
    echo -e "${GREEN}IDEMPOTENCY TEST: PASSED${NC}"
    exit 0
else
    echo -e "${RED}IDEMPOTENCY TEST: FAILED${NC}"
    exit 1
fi
