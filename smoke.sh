#!/bin/bash
# End-to-end smoke test: real ZooKeeper + 3 brokers, then byte-compare every
# replica's log and check the cluster's own invariants.
#
# What it asserts:
#   1. every message produced comes back out of the consumers
#   2. every replica of a partition holds a byte-identical log
#   3. brokers outside a partition's replica set created no log at all
#   4. exactly one broker wins the controller election, with no failures logged
#   5. no SEVERE line anywhere
#
# It runs on its own ZooKeeper and its own broker ports rather than sharing
# demo.sh's (2181 and 9091-9093), so it can be run with the demo cluster still
# up and never stops anything it did not start. Override with SMOKE_ZK_PORT and
# SMOKE_BASE_PORT.
#
# Usage: ./smoke.sh          (needs Docker for ZooKeeper)
set -u
cd "$(dirname "$0")"

JAR=target/build-your-own-kafka-1.0-SNAPSHOT.jar
TOPIC="smoke-$(date +%s)"
WORK="${SMOKE_WORK:-$PWD/target/smoke}"
ZK_CONTAINER="${SMOKE_ZK_CONTAINER:-zookeeper-smoke}"
ZK_PORT="${SMOKE_ZK_PORT:-12181}"
BASE_PORT="${SMOKE_BASE_PORT:-19090}"
rm -rf "$WORK"; mkdir -p "$WORK"

cleanup() {
  if [ -f "$WORK/pids" ]; then
    xargs -r kill < "$WORK/pids" 2>/dev/null
    sleep 2
    xargs -r kill -9 < "$WORK/pids" 2>/dev/null
  fi
  docker rm -f "$ZK_CONTAINER" >/dev/null 2>&1
  rm -rf data/1/smoke-* data/2/smoke-* data/3/smoke-*
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || { echo "ERROR: docker is required" >&2; exit 1; }

# Rebuild when the tree has moved on, so a run never reports on a stale jar.
if [ ! -f "$JAR" ] || [ -n "$(find src pom.xml -newer "$JAR" -print -quit 2>/dev/null)" ]; then
  echo "building $JAR ..."
  mvn -q -DskipTests package
fi
[ -f "$JAR" ] || { echo "ERROR: $JAR not found" >&2; exit 1; }

# Refuse to start on occupied ports. Without this a leftover broker shows up as
# three identical bind failures buried in the broker logs, and the run reports
# the misleading "nothing was produced" instead of the real cause.
BUSY=""
for port in "$ZK_PORT" $(seq $((BASE_PORT + 1)) $((BASE_PORT + 3))); do
  if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
    echo "ERROR: port $port is already in use:" >&2
    ss -ltnp 2>/dev/null | grep -E "[:.]$port\b" | sed 's/^/  /' >&2
    BUSY=1
  fi
done
if [ -n "$BUSY" ]; then
  echo "the smoke test needs $ZK_PORT and $((BASE_PORT + 1))-$((BASE_PORT + 3)) to itself." >&2
  echo "stop whatever holds them, or point SMOKE_BASE_PORT / SMOKE_ZK_PORT at free ports." >&2
  exit 1
fi

# This is smoke's own container, so clearing a leftover from an interrupted run
# is ours to do - the demo cluster's ZooKeeper is a different container entirely.
docker rm -f "$ZK_CONTAINER" >/dev/null 2>&1
docker run -d --name "$ZK_CONTAINER" -p "$ZK_PORT":2181 zookeeper:3.8 >/dev/null
for i in $(seq 1 40); do
  docker exec "$ZK_CONTAINER" zkServer.sh status 2>/dev/null | grep -q Mode && break
  sleep 1
done
sleep 2
echo "zookeeper up (localhost:$ZK_PORT)"

for i in 1 2 3; do
  port=$((BASE_PORT + i))
  nohup java -cp "$JAR" com.simplekafka.broker.SimpleKafkaBroker "$i" localhost "$port" "$ZK_PORT" \
      > "$WORK/broker$i.log" 2>&1 &
  echo $! >> "$WORK/pids"
done

# Wait for each broker to actually listen. A fixed sleep cannot tell a started
# broker from one that died on the way up, which is exactly how a bind failure
# turned into "produced: 0".
wait_for_broker() {
  local i=$1
  local port=$((BASE_PORT + i))
  local pid
  local reason="did not listen within 30s"
  pid=$(sed -n "${i}p" "$WORK/pids")
  for _ in $(seq 1 60); do
    (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null && return 0
    if ! kill -0 "$pid" 2>/dev/null; then
      reason="exited during startup"
      break
    fi
    sleep 0.5
  done
  echo "  broker $i (localhost:$port) $reason:"
  tail -20 "$WORK/broker$i.log" 2>/dev/null | sed 's/^/    /'
  return 1
}

brokers_up=1
for i in 1 2 3; do
  wait_for_broker "$i" || brokers_up=0
done
if [ "$brokers_up" != 1 ]; then
  echo "SMOKE FAILED (brokers did not start)"
  exit 1
fi
# Listening means the socket is bound; give the election and topic load a moment
# to settle before the clients arrive, as the fixed sleep here used to.
sleep 5
echo "brokers up (localhost:$((BASE_PORT + 1))-$((BASE_PORT + 3)))"

fail=0

echo "--- produce (20 messages, 3 partitions, replication factor 2) ---"
for round in 1 2; do
  java -cp "$JAR" com.simplekafka.client.SimpleKafkaProducer localhost $((BASE_PORT + 1)) "$TOPIC" \
      > "$WORK/producer-$round.log" 2>&1
done
produced=$(grep -hc "^Sent message to offset" "$WORK"/producer-*.log | paste -sd+ | bc)
echo "  produced: $produced"
[ "$produced" -gt 0 ] || { echo "  nothing was produced"; fail=1; }
sleep 5

echo "--- consume from each partition ---"
consumed=0
for p in 0 1 2; do
  sleep 5 | java -cp "$JAR" com.simplekafka.client.SimpleKafkaConsumer localhost $((BASE_PORT + 1)) "$TOPIC" "$p" \
      > "$WORK/consumer-$p.log" 2>&1
  n=$(grep -c 'Received message' "$WORK/consumer-$p.log")
  echo "  partition $p: $n message(s) consumed"
  consumed=$((consumed + n))
done
echo "  consumed: $consumed / produced: $produced"
if [ "$consumed" != "$produced" ]; then
  echo "  MISMATCH: consumers did not return everything that was produced"
  fail=1
fi

echo "--- byte-comparing replica logs (only brokers that actually hold the partition) ---"
assignments=$(grep -h "Created partition .* for topic $TOPIC with leader" "$WORK"/broker*.log \
  | sed -E 's/.*Created partition ([0-9]+) .* leader ([0-9]+) and followers \[([0-9,]*)\].*/\1 \2 \3/')
if [ -z "$assignments" ]; then
  echo "  could not find partition assignment in the broker logs"
  fail=1
fi
while read -r p leader followers; do
  [ -z "${p:-}" ] && continue
  replicas=$(echo "$leader $followers" | tr ',' ' ')
  echo "  partition $p: replicas = [$replicas]"
  refs=()
  for b in $replicas; do
    f="data/$b/$TOPIC/$p/00000000000000000000.log"
    if [ -f "$f" ]; then refs+=("$b:$f"); else echo "  partition $p: replica $b has NO log"; fail=1; fi
  done
  if [ ${#refs[@]} -lt 2 ]; then
    echo "  partition $p: fewer than two replicas"
    fail=1
    continue
  fi
  ref=${refs[0]}
  for other in "${refs[@]:1}"; do
    if cmp -s "${ref#*:}" "${other#*:}"; then
      echo "  partition $p: replica ${ref%%:*} == replica ${other%%:*} identical ($(stat -c%s "${ref#*:}") bytes)"
    else
      echo "  partition $p: MISMATCH replica ${ref%%:*} vs replica ${other%%:*}"
      fail=1
    fi
  done

  # Brokers outside the replica set must not have created anything at all.
  for b in 1 2 3; do
    case " $replicas " in *" $b "*) continue ;; esac
    d="data/$b/$TOPIC/$p"
    if [ -e "$d" ]; then
      echo "  partition $p: broker $b is not a replica but created $d"
      fail=1
    fi
  done
done <<< "$assignments"

echo "--- controller election (must be one winner, no failures) ---"
winners=$(grep -hc "This broker is now the active controller" "$WORK"/broker*.log | paste -sd+ | bc)
echo "  brokers claiming controllership: $winners"
if [ "$winners" != "1" ]; then fail=1; fi
if grep -q "Controller election failed" "$WORK"/broker*.log; then
  echo "  controller election reported failures:"
  grep -h "Controller election failed" "$WORK"/broker*.log | sed 's/^/    /'
  fail=1
fi

echo "--- other errors ---"
other_severe=$(grep -h "SEVERE" "$WORK"/broker*.log | sort -u | head -20 || true)
if [ -n "$other_severe" ]; then
  echo "$other_severe" | sed 's/^/  /'
  fail=1
else
  echo "  none"
fi

if [ "$fail" -ne 0 ]; then echo "SMOKE FAILED"; exit 1; fi
echo "SMOKE OK (topic $TOPIC)"
