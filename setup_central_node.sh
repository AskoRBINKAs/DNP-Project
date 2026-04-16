#!/bin/bash


echo "[*] Starting containers"

docker compose -f docker-compose.central_node.yml up -d
docker compose -f docker-compose.central_node.yml exec -T manticore mysql -h127.0.0.1 -P9306 < manticore/create_tables.sql
sleep 10

echo "[*] Setting up RabbitMQ"

RABBITMQ_PASSWORD="$(openssl rand -base64 24)"
echo "[*] Generated password for logger: ${RABBITMQ_PASSWORD}"

docker compose -f docker-compose.central_node.yml exec rabbitmq rabbitmqctl add_vhost logs
docker compose -f docker-compose.central_node.yml exec rabbitmq rabbitmqctl add_user logger "${RABBITMQ_PASSWORD}"
docker compose -f docker-compose.central_node.yml exec rabbitmq rabbitmqctl set_permissions -p logs logger ".*" ".*" ".*"

echo "[+] Enjoy ;)" 