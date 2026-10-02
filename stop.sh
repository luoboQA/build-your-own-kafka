#!/bin/bash
# Stop the demo cluster: ZooKeeper and every broker started by demo.sh.

docker rm -f zookeeper 2>/dev/null || true

# -f matches the whole command line, so the pattern has to describe a broker and
# nothing else. A bare "SimpleKafkaBroker" also matches anything that merely mentions
# the name - a shell running a command containing it, a tail on a broker log, an
# editor searching for it - and this script is meant to stop brokers, not those.
# demo.sh starts them as "nohup java -cp <jar> com.simplekafka.broker.SimpleKafkaBroker ...",
# so anchoring on the interpreter is what tells the two apart.
pkill -f '(^|/)java .*com\.simplekafka\.broker\.SimpleKafkaBroker' 2>/dev/null || true

echo "已停止"
