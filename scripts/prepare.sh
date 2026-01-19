#!/bin/bash
# Предназначен для случаев, когда PostgreSQL уже установлен и запущен локально

set -euo pipefail

echo "Подготовка проекта (PostgreSQL считается уже установленным локально)"

# 1. Инициализация Go-модуля, если его ещё нет
if [ ! -f go.mod ]; then
    echo "Инициализация Go-модуля..."
    go mod init prices-api || true
fi

# Установка / обновление зависимостей
echo "Установка зависимостей..."
go mod tidy

# 2. Проверка, что PostgreSQL доступен
echo "Проверка подключения к PostgreSQL..."

if ! pg_isready -h localhost -p 5432 -U validator -d project-sem-1 -t 5 >/dev/null 2>&1; then
    echo "Ошибка: PostgreSQL не запущен или недоступен на localhost:5432"
    echo ""
    echo "Убедитесь, что:"
    echo "  • PostgreSQL запущен"
    echo "  • Пользователь validator существует"
    echo "  • Пароль: val1dat0r"
    echo "  • База project-sem-1 создана"
    echo ""
    echo "Запустите PostgreSQL и создайте пользователя/базу, если нужно:"
    echo "  sudo systemctl start postgresql"
    echo "  sudo -u postgres createuser validator"
    echo "  sudo -u postgres psql -c \"ALTER USER validator WITH PASSWORD 'val1dat0r';\""
    echo "  sudo -u postgres createdb -O validator project-sem-1"
    exit 1
fi

# 3. Создание таблицы (если не существует)
echo "Создание таблицы prices (если отсутствует)..."

PGPASSWORD=val1dat0r psql -h localhost -p 5432 -U validator -d project-sem-1 -c "
CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    create_date TIMESTAMP NOT NULL
);

-- уникальность записи: совпадение всех полей кроме id
CREATE UNIQUE INDEX IF NOT EXISTS prices_unique_idx
ON prices (name, category, price, create_date);
" || {
    echo "Ошибка при создании таблицы"
    exit 1
}

echo ""
echo "Подготовка завершена успешно"
echo "Теперь можно запустить сервер командой:"
echo "  ./scripts/run.sh"
echo ""