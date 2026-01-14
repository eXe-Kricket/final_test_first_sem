#!/bin/bash

# Установка зависимостей Go
go mod init prices-server
go mod tidy

# Ждем, пока БД запустится
sleep 5

# Создаем таблицу (но в коде Go это уже есть, здесь для подготовки)
psql -h localhost -p 5432 -U validator -d project-sem-1 -c "CREATE TABLE IF NOT EXISTS prices (id SERIAL PRIMARY KEY, item TEXT, category TEXT, price INTEGER);"