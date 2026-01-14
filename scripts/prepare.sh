#!/bin/bash

set -e

# Инициализация модулей (если go.mod ещё нет)
if [ ! -f go.mod ]; then
    go mod init prices-api
fi

go mod tidy

# Запуск PostgreSQL в Docker, если не запущен
if ! docker ps | grep -q postgres-db; then
    docker run --name postgres-db \
        -e POSTGRES_USER=validator \
        -e POSTGRES_PASSWORD=val1dat0r \
        -e POSTGRES_DB=project-sem-1 \
        -p 5432:5432 \
        -d postgres:latest
fi

# Ждём, пока PostgreSQL полностью запустится
echo "Waiting for PostgreSQL to start..."
sleep 12  # обычно хватает 8–15 сек

# Создаём таблицу с передачей пароля
PGPASSWORD=val1dat0r psql -h localhost -p 5432 -U validator -d project-sem-1 <<EOF
CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    item TEXT,
    category TEXT,
    price INTEGER
);
EOF

echo "Database prepared successfully."