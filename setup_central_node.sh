#!/bin/bash

RABBITMQ_PASSWORD="$(openssl rand -base64 24)"
GRAFANA_PASSWORD="$(openssl rand -base64 24)"

touch .env.collector

echo "RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_QUEUE=logs" > .env.collector
echo "RABBITMQ_USER=logger" >> .env.collector
echo "RABBITMQ_PASSWORD=${RABBITMQ_PASSWORD}" >> .env.collector
echo "[*] Starting RabbitMQ and ManticoreSearch"

docker compose -f docker-compose.central_node.yml up manticore rabbitmq -d
docker compose -f docker-compose.central_node.yml exec -T manticore mysql -h127.0.0.1 -P9306 < manticore/create_tables.sql
sleep 10

echo "[*] Setting up RabbitMQ"
echo "[*] Generated password for logger: ${RABBITMQ_PASSWORD}"
docker compose -f docker-compose.central_node.yml exec rabbitmq rabbitmqctl add_vhost logs
docker compose -f docker-compose.central_node.yml exec rabbitmq rabbitmqctl add_user logger "${RABBITMQ_PASSWORD}"
docker compose -f docker-compose.central_node.yml exec rabbitmq rabbitmqctl set_permissions -p logs logger ".*" ".*" ".*"



echo "[*] Building Log Collector"

docker compose -f docker-compose.central_node.yml up --build -d

echo "[+] Enjoy ;)" 
