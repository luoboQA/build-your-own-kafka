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
# Usage: ./smoke.sh          (needs Docker for ZooKeeper)
set -u
cd "$(dirname "$0")"

JAR=target/build-your-own-kafka-1.0-SNAPSHOT.jar
TOPIC="smoke-$(date +%s)"
WORK="${SMOKE_WORK:-$PWD/target/smoke}"
rm -rf "$WORK"; mkdir -p "$WORK"

cleanup() {
  [ -f "$WORK/pids" ] && xargs -r kill < "$WORK/pids" 2>/dev/null
  sleep 2
  xargs -r kill -9 < "$WORK/pids" 2>/dev/null
  docker rm -f zookeeper >/dev/null 2>&1
  rm -rf data/1/smoke-* data/2/smoke-* data/3/smoke-*
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || { echo "ERROR: docker is required" >&2; exit 1; }

if [ ! -f "$JAR" ]; then
  echo "building $JAR ..."
  mvn -q -DskipTests package
fi
[ -f "$JAR" ] || { echo "ERROR: $JAR not found" >&2; exit 1; }

docker rm -f zookeeper >/dev/null 2>&1
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.8 >/dev/null
for i in $(seq 1 40); do
  docker exec zookeeper zkServer.sh status 2>/dev/null | grep -q Mode && break
  sleep 1
done
sleep 2
echo "zookeeper up"

for i in 1 2 3; do
  port=$((9090 + i))
  nohup java -cp "$JAR" com.simplekafka.broker.SimpleKafkaBroker "$i" localhost "$port" 2181 \
      > "$WORK/broker$i.log" 2>&1 &
  echo $! >> "$WORK/pids"
done
sleep 6
echo "brokers up"

fail=0

echo "--- produce (20 messages, 3 partitions, replication factor 2) ---"
for round in 1 2; do
  java -cp "$JAR" com.simplekafka.client.SimpleKafkaProducer localhost 9091 "$TOPIC" \
      > "$WORK/producer-$round.log" 2>&1
done
produced=$(grep -hc "^Sent message to offset" "$WORK"/producer-*.log | paste -sd+ | bc)
echo "  produced: $produced"
[ "$produced" -gt 0 ] || { echo "  nothing was produced"; fail=1; }
sleep 5

echo "--- consume from each partition ---"
consumed=0
for p in 0 1 2; do
  sleep 5 | java -cp "$JAR" com.simplekafka.client.SimpleKafkaConsumer localhost 9091 "$TOPIC" "$p" \
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
