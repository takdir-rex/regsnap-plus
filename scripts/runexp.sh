#!/bin/bash
docker exec redis redis-cli flushall
docker exec kafka /data/shared/kafka/bin/kafka-topics.sh --create --topic "$5" --bootstrap-server localhost:9092 --partitions 1
args=$(echo "--rps $4 --statesize 50 --stateratio 20,20,20,20,20 --sg $2 --topic $5")
data=$(echo "{\"parallelism\":$3, \"programArgs\":\"${args}\"}")
curl --location "http://192.168.137.11:8081/jars/$1/run" --header "Content-Type: application/json" --data "$data"
