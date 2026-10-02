#!/bin/bash

JAR="target/build-your-own-kafka-1.0-SNAPSHOT.jar"


mvn clean package -q

# 清理并启动 ZooKeeper
docker rm -f zookeeper 2>/dev/null || true
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.8 > /dev/null

# 等待 ZooKeeper 完全就绪
echo "等待 ZooKeeper 启动..."
for i in {1..20}; do
    if docker exec zookeeper zkServer.sh status 2>/dev/null | grep -q "Mode"; then
        echo "ZooKeeper 已就绪"
        break
    fi
    sleep 1
done

sleep 2

# 启动 3 个 Broker
mkdir -p logs
for i in 1 2 3; do
    port=$((9090 + i))
    echo "启动 Broker $i..."
    nohup java -cp "$JAR" com.simplekafka.broker.SimpleKafkaBroker \
        "$i" localhost "$port" 2181 > "logs/broker$i.log" 2>&1 &
done

sleep 5
echo "集群已启动！"
echo "发送消息: java -cp $JAR com.simplekafka.client.SimpleKafkaProducer localhost 9091 test-topic"
echo "消费消息: java -cp $JAR com.simplekafka.client.SimpleKafkaConsumer localhost 9091 test-topic 0"
echo "从指定 offset 消费: java -cp $JAR com.simplekafka.client.SimpleKafkaConsumer localhost 9091 test-topic 0 5"