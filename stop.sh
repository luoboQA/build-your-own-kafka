#!/bin/bash
docker rm -f zookeeper 2>/dev/null || true
pkill -f SimpleKafkaBroker 2>/dev/null || true
echo "已停止"