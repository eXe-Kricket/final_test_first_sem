#!/usr/bin/env bash

set -euo pipefail

echo "Подготовка проекта (локальный PostgreSQL@16 от Homebrew)"

# 1. Go-зависимости
if [ ! -f go.mod ]; then
    echo "Инициализация модуля..."
    go mod init prices-api || true
fi

echo "Установка/обновление зависимостей..."
go mod tidy

# 2. Убедимся, что PostgreSQL запущен
if ! brew services list | grep -q "postgresql@16.*started"; then
    echo "Запускаем postgresql@16..."
    brew services start postgresql@16
    sleep 5
fi

# 3. Создаём роль validator (это можно в DO-блоке)
echo "Создаём роль validator (если не существует)..."

psql -d postgres -c "
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'validator') THEN
      CREATE ROLE validator WITH LOGIN PASSWORD 'val1dat0r';
      ALTER ROLE validator CREATEDB;
      RAISE NOTICE 'Роль validator создана';
   ELSE
      RAISE NOTICE 'Роль validator уже существует';
   END IF;
END
\$\$;
"

# 4. Создаём базу project-sem-1 (ОТДЕЛЬНАЯ команда, НЕ внутри DO!)
echo "Создаём базу project-sem-1 (если не существует)..."

# Пытаемся создать → если уже есть, psql вернёт ошибку 42P04, которую игнорируем
psql -d postgres -c "CREATE DATABASE \"project-sem-1\" OWNER validator;" || true

# 5. Даём привилегии (на всякий случай, если база уже была)
psql -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE \"project-sem-1\" TO validator;"

# 6. Теперь создаём таблицу внутри базы
echo "Создаём таблицу prices (если не существует)..."

PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1 -c "
CREATE TABLE IF NOT EXISTS prices (
    id       SERIAL PRIMARY KEY,
    item     TEXT NOT NULL,
    category TEXT NOT NULL,
    price    INTEGER NOT NULL
);
"

echo ""
echo "Подготовка завершена успешно!"
echo "Теперь можно запускать сервер:"
echo "  go run main.go"
echo "или"
echo "  ./scripts/run.sh"
echo ""
echo "Проверить подключение можно командой:"
echo "  PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1"