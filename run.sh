#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

: "${MT_LICENSE:?Export your own MT_LICENSE before running}"
suffix="mt-key-$(date +%s)-${RANDOM}"
postgres="${suffix}-postgres"
kafka="${suffix}-kafka"
postgres_port=$((20000 + RANDOM % 10000))
kafka_port=$((40000 + RANDOM % 10000))
containers=()
cleanup() {
    if ((${#containers[@]})); then
        docker rm -fv "${containers[@]}" >/dev/null \
            || echo "Could not remove disposable containers: ${containers[*]}" >&2
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

containers+=("$(docker create --name "$postgres" -p "127.0.0.1:${postgres_port}:5432" \
    -e POSTGRES_DB=repro -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17.6)")
docker start "${containers[0]}" >/dev/null
containers+=("$(docker create --name "$kafka" -p "127.0.0.1:${kafka_port}:9092" \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,INTERNAL://:19092,CONTROLLER://:9093 \
    -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:${kafka_port},INTERNAL://localhost:19092" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:4.0.1)")
docker start "${containers[1]}" >/dev/null

ready=false
for ((attempt=0; attempt<60; attempt++)); do
    if docker exec "$postgres" pg_isready -U postgres -d repro >/dev/null 2>&1 \
        && docker exec "$kafka" timeout 5 /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server localhost:19092 --list >/dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done
if [[ "$ready" != true ]]; then
    echo "Infrastructure did not become ready." >&2
    exit 2
fi

export KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:${kafka_port}"
export POSTGRES_CONNECTION="Host=127.0.0.1;Port=${postgres_port};Database=repro;Username=postgres"
dotnet restore --locked-mode --verbosity quiet
dotnet run --no-restore
