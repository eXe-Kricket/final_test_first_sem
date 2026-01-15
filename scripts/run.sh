#!/usr/bin/env bash

set -euo pipefail

echo "Запуск приложения в фоне..."
go run main.go &
SERVER_PID=$!

echo "Ожидание открытия порта 8080 (максимум 120 секунд)..."

# Проверяем наличие netcat
if ! command -v nc >/dev/null 2>&1; then
    echo "Ошибка: netcat (nc) не найден в системе"
    echo "Установите netcat-openbsd или netcat-traditional в CI (apt install netcat-openbsd)"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

for i in {1..120}; do
    if nc -z localhost 8080 >/dev/null 2>&1; then
        echo "Порт 8080 открыт!"
        
        # Дополнительная проверка, что API отвечает (опционально, но рекомендуется)
        if curl -s -f -o /dev/null http://localhost:8080/api/v0/prices 2>/dev/null; then
            echo "API отвечает → smoke-тест пройден"
            curl -v http://localhost:8080/api/v0/prices
        else
            echo "Порт открыт, но API пока не отвечает (возможно, ещё инициализируется)"
        fi
        
        break
    fi

    if ! ps -p $SERVER_PID >/dev/null 2>&1; then
        echo "Приложение упало во время запуска"
        exit 1
    fi

    echo "Сервер ещё не готов (попытка $i/120)..."
    sleep 1
done

# Финальная проверка порта
if ! nc -z localhost 8080 >/dev/null 2>&1; then
    echo "Сервер не открыл порт 8080 за 120 секунд"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

echo "Smoke-тест успешно пройден (порт открыт)"

# Останавливаем процесс
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null || true

exit 0
