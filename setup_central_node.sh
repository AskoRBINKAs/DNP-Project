#!/bin/bash


echo "[*] Starting containers"

docker compose -f docker-compose.central_node.yml up -d
docker compose exec -T manticore mysql -h127.0.0.1 -P9306 < manticore/create_tables.sql

echo "[+] Enjoy ;)" 