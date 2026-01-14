#!/bin/bash

# Установка зависимостей Go
go mod init prices-server
go mod tidy

# Предполагаем, что PostgreSQL установлен локально или через Docker
# Запускаем PostgreSQL через Docker, если не запущен
if ! docker ps | grep -q postgres; then
    docker run --name postgres-db -e POSTGRES_PASSWORD=val1dat0r -e POSTGRES_USER=validator -e POSTGRES_DB=project-sem-1 -p 5432:5432 -d postgres
fi

# Ждем, пока БД запустится
sleep 5

# Создаем таблицу (но в коде Go это уже есть, здесь для подготовки)
psql -h localhost -p 5432 -U validator -d project-sem-1 -c "CREATE TABLE IF NOT EXISTS prices (id SERIAL PRIMARY KEY, item TEXT, category TEXT, price INTEGER);"