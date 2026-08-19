#!/usr/bin/env bash
# ============================================================
# Деплой Kafka-кластера в Yandex Cloud — оркестратор
#
# Шаги:
#   1. Проверка окружения (ansible, ssh-ключ, .env)
#   2. Проверка SSH-доступа до всех VM
#   3. Установка Ansible-коллекций
#   4. Деплой (ansible-playbook)
#   5. Health check
#   6. Опционально: idempotency / e2e / failure тесты
#
# Использование:
#   ./deploy.sh                     # полный деплой
#   ./deploy.sh --kafka-only        # только Kafka (без ClickHouse)
#   ./deploy.sh --check             # только проверки, без деплоя
#   ./deploy.sh --test-idempotency  # деплой + тест идемпотентности
#   ./deploy.sh --test-e2e          # деплой + end-to-end тест Kafka->ClickHouse
#   ./deploy.sh --test-failure      # деплой + тест отказов
# ============================================================
set -euo pipefail

# --- Цвета ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# --- Конфигурация ---
# deploy.sh лежит в корне проекта, поэтому PROJECT_DIR == SCRIPT_DIR
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}"
ANSIBLE_DIR="${PROJECT_DIR}/ansible"
INVENTORY="${ANSIBLE_DIR}/inventories/yandex/hosts.yml"
PLAYBOOK="${ANSIBLE_DIR}/playbooks/site.yml"
ENV_FILE="${PROJECT_DIR}/.env"
SSH_KEY_DEFAULT="${HOME}/.ssh/id_ed25519"

# --- Флаги ---
RUN_DEPLOY=true
KAFKA_ONLY=false
RUN_IDEMPOTENCY=false
RUN_E2E=false
RUN_FAILURE=false

for arg in "$@"; do
    case "$arg" in
        --check)           RUN_DEPLOY=false ;;
        --kafka-only)      KAFKA_ONLY=true ;;
        --test-idempotency) RUN_IDEMPOTENCY=true ;;
        --test-e2e)        RUN_E2E=true ;;
        --test-failure)    RUN_FAILURE=true ;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *)
            echo "Неизвестный аргумент: $arg"
            sed -n '2,30p' "$0"
            exit 1
            ;;
    esac
done

log()  { echo -e "${BLUE}[DEPLOY]${NC} $1"; }
info() { echo -e "${GREEN}[ OK ]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

# ============================================================
# Шаг 0: Проверка окружения
# ============================================================
echo "============================================================"
echo "  Деплой Kafka-кластера в Yandex Cloud"
echo "============================================================"
cd "${PROJECT_DIR}"

# 0.1 Ansible
log "Проверка Ansible..."
if ! command -v ansible >/dev/null 2>&1; then
    fail "Ansible не установлен. Установите: pip3 install ansible-core"
fi
info "Ansible: $(ansible --version | head -1)"

# 0.2 SSH-ключ
log "Проверка SSH-ключа..."
SSH_KEY="${ANSIBLE_SSH_PRIVATE_KEY:-${SSH_KEY_DEFAULT}}"
SSH_KEY="${SSH_KEY//\~/${HOME}}"
if [ ! -f "${SSH_KEY}" ]; then
    fail "SSH-ключ не найден: ${SSH_KEY}. Создайте: ssh-keygen -t ed25519 -f ${SSH_KEY_DEFAULT}"
fi
if [ "${SSH_KEY##*.}" != "pub" ] && [ ! -f "${SSH_KEY}.pub" ]; then
    warn "Публичный ключ ${SSH_KEY}.pub не найден — нужно добавить его на VM при создании"
fi
info "SSH-ключ: ${SSH_KEY}"

# 0.3 .env файл
log "Проверка .env..."
if [ ! -f "${ENV_FILE}" ]; then
    warn ".env не найден. Создаю из шаблона..."
    cp "${PROJECT_DIR}/.env.example" "${ENV_FILE}"
    info "Создан ${ENV_FILE}. ОТКРОЙТЕ ЕГО И ЗАПОЛНИТЕ IP-адреса!"
    echo ""
    echo "  vim ${ENV_FILE}"
    echo ""
    exit 0
fi

# 0.4 source .env
log "Загрузка переменных из .env..."
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

# 0.5 Валидация IP-адресов
log "Проверка IP-адресов..."
REQUIRED_IPS=(
    "KAFKA_1_PUBLIC_IP"
    "KAFKA_2_PUBLIC_IP"
    "KAFKA_3_PUBLIC_IP"
    "KAFKA_1_IP"
    "KAFKA_2_IP"
    "KAFKA_3_IP"
)

is_valid_ip() {
    local ip="$1"
    [[ "${ip}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
    local IFS='.'
    read -ra octets <<< "${ip}"
    for octet in "${octets[@]}"; do
        [ "${octet}" -ge 0 ] && [ "${octet}" -le 255 ] || return 1
    done
    return 0
}

for var in "${REQUIRED_IPS[@]}"; do
    val="${!var:-}"
    if [ -z "${val}" ]; then
        fail "Переменная ${var} не задана в ${ENV_FILE}"
    fi
    if ! is_valid_ip "${val}"; then
        fail "Переменная ${var} содержит некорректный IP: '${val}' — проверьте ${ENV_FILE}"
    fi
done

# Предупреждение о дефолтных (заглушечных) IP из шаблона
# ВАЖНО: матчим только ТОЧНЫЕ заглушки из .env.example, а не диапазоны —
# реальные IP могут начинаться с тех же префиксов (158.160.x.x, 10.0.x.x).
for var in KAFKA_1_PUBLIC_IP KAFKA_2_PUBLIC_IP KAFKA_3_PUBLIC_IP KAFKA_1_IP KAFKA_2_IP KAFKA_3_IP; do
    val="${!var:-}"
    case "${val}" in
        158.160.1.10|158.160.1.11|158.160.1.12|158.160.1.13|10.128.0.5|10.128.0.6|10.128.0.7|10.128.0.8|10.0.0.1|10.0.0.2|10.0.0.3)
            warn "  ${var} = ${val} это заглушка из шаблона — проверьте, что .env заполнен реальными IP"
            ;;
    esac
done
info "IP-адреса заданы"

# ============================================================
# Шаг 1: Проверка SSH-доступа до всех VM
# ============================================================
echo ""
log "Проверка SSH-доступа до VM..."

check_ssh() {
    local host="$1"
    local label="$2"
    log "  → ${label} (${host})"
    if ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes \
        "ubuntu@${host}" "hostname" >/dev/null 2>&1; then
        info "  ${label}: SSH OK"
    else
        warn "  ${label}: SSH недоступен (${host}). Проверьте IP и Security Group."
        return 1
    fi
}

SSH_FAILED=0
check_ssh "${KAFKA_1_PUBLIC_IP}" "kafka-1" || SSH_FAILED=1
check_ssh "${KAFKA_2_PUBLIC_IP}" "kafka-2" || SSH_FAILED=1
check_ssh "${KAFKA_3_PUBLIC_IP}" "kafka-3" || SSH_FAILED=1

# ClickHouse необязателен: проверяем только если IP задан и не является заглушкой
CH_IS_PLACEHOLDER=false
case "${CLICKHOUSE_PUBLIC_IP:-}" in
    ""|158.160.1.13|10.128.0.8) CH_IS_PLACEHOLDER=true ;;
esac
if [ "${CH_IS_PLACEHOLDER}" = "false" ]; then
    check_ssh "${CLICKHOUSE_PUBLIC_IP}" "clickhouse-1" || SSH_FAILED=1
else
    warn "ClickHouse VM не обнаружена (IP не задан/заглушка) — ClickHouse будет пропущен"
fi

if [ "${SSH_FAILED}" = "1" ]; then
    echo ""
    fail "Некоторые VM недоступны по SSH. Проверьте:"
    echo "  1. Публичный ключ ~/.ssh/id_ed25519.pub добавлен на VM (Metadata)"
    echo "  2. Security Group Yandex Cloud разрешает TCP 22 с вашего IP"
    echo "  3. IP-адреса в .env корректны"
fi

# ============================================================
# Шаг 2: Установка Ansible-коллекций
# ============================================================
echo ""
log "Установка Ansible-коллекций..."
cd "${ANSIBLE_DIR}"
ansible-galaxy collection install -r requirements.yml --force 2>&1 | tail -2 || \
    warn "Не удалось установить коллекции — проверьте подключение к интернету"

# ============================================================
# Шаг 3: Проверка подключения через Ansible
# ============================================================
echo ""
log "Проверка подключения через Ansible ping..."
ansible all -i "${INVENTORY}" -m ping --private-key "${SSH_KEY}" 2>&1 | tail -8

# ============================================================
# Шаг 4: Деплой
# ============================================================
if [ "${RUN_DEPLOY}" = "true" ]; then
    echo ""
    log "Запуск деплоя..."

    if [ "${KAFKA_ONLY}" = "true" ] || [ "${CH_IS_PLACEHOLDER}" = "true" ]; then
        if [ "${KAFKA_ONLY}" = "true" ]; then
            log "Режим: только Kafka (без ClickHouse) — флаг --kafka-only"
        else
            log "ClickHouse VM не настроена (IP-заглушка) — деплою только Kafka"
        fi
        ansible-playbook -i "${INVENTORY}" "${PLAYBOOK}" --tags kafka
    else
        ansible-playbook -i "${INVENTORY}" "${PLAYBOOK}"
    fi

    if [ $? -eq 0 ]; then
        info "Деплой завершён успешно"
    else
        fail "Деплой завершился с ошибкой"
    fi
else
    log "Режим --check: деплой пропущен"
fi

# ============================================================
# Шаг 5: Health check
# ============================================================
echo ""
log "Запуск health check..."
if [ -f "${ANSIBLE_DIR}/scripts/health_check.sh" ]; then
    if [ -n "${KAFKA_1_PUBLIC_IP:-}" ]; then
        bash "${ANSIBLE_DIR}/scripts/health_check.sh" "${KAFKA_1_PUBLIC_IP}" 9092 kafka-broker || \
            warn "Health check предупредил о проблемах — проверьте логи"
    fi
fi

# ============================================================
# Шаг 6: Опциональные тесты
# ============================================================
if [ "${RUN_IDEMPOTENCY}" = "true" ]; then
    echo ""
    log "Запуск теста идемпотентности..."
    bash "${ANSIBLE_DIR}/scripts/test_idempotency.sh" "${INVENTORY}" "${PLAYBOOK}"
fi

if [ "${RUN_E2E}" = "true" ]; then
    echo ""
    log "Запуск end-to-end теста Kafka → ClickHouse..."
    if ! command -v python3 >/dev/null 2>&1; then
        fail "python3 не найден"
    fi
    pip install --quiet kafka-python requests 2>/dev/null || true
    python3 "${ANSIBLE_DIR}/scripts/test_e2e.py" \
        --kafka-brokers "${KAFKA_1_IP}:9092,${KAFKA_2_IP}:9092,${KAFKA_3_IP}:9092" \
        --clickhouse-host "${CLICKHOUSE_IP:-}" \
        --count 10000
fi

if [ "${RUN_FAILURE}" = "true" ]; then
    echo ""
    log "Запуск теста отказов..."
    bash "${ANSIBLE_DIR}/scripts/test_failure.sh" "${INVENTORY}"
fi

# ============================================================
# Итог
# ============================================================
echo ""
echo "============================================================"
echo "  Итог:"
echo "============================================================"
echo "  Клиентские порты Kafka: ${KAFKA_1_IP}:9092, ${KAFKA_2_IP}:9092, ${KAFKA_3_IP}:9092"
echo "  Топик: analytics.events"
echo ""
echo "  Быстрые команды:"
echo "    Топики:  ssh -i ${SSH_KEY} ubuntu@${KAFKA_1_PUBLIC_IP} \\
      \"docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \\
      --bootstrap-server localhost:9092 --list\""
if [ -n "${CLICKHOUSE_PUBLIC_IP:-}" ]; then
    echo "    ClickHouse: ssh -i ${SSH_KEY} ubuntu@${CLICKHOUSE_PUBLIC_IP} \\
      'docker exec clickhouse clickhouse-client --query \"SELECT count() FROM analytics.events\"'"
fi
echo "============================================================"

exit 0