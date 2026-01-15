#!/usr/bin/env bash

set -euo pipefail

echo "Запуск приложения в фоне..."
go run main.go &
SERVER_PID=$!

echo "Ожидание открытия порта 8080 (максимум 60 секунд)..."

for i in {1..60}; do
  if curl -s -f -o /dev/null http://localhost:8080/api/v0/prices 2>/dev/null; then
    echo "Сервер отвечает → smoke-тест пройден"
    # Делаем реальный запрос для лога
    curl -v http://localhost:8080/api/v0/prices
    break
  fi

  if ! ps -p $SERVER_PID > /dev/null; then
    echo "Приложение упало во время запуска"
    exit 1
  fi

  echo "Сервер ещё не готов (попытка $i/60)..."
  sleep 1
done

# Проверяем, запустился ли в итоге
if ! curl -s -f -o /dev/null http://localhost:8080/api/v0/prices 2>/dev/null; then
  echo "Сервер не запустился за 60 секунд (или порт не открыт)"
  kill $SERVER_PID 2>/dev/null || true
  exit 1
fi

echo "Smoke-тест успешно пройден"

# Останавливаем процесс
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null || true

exit 0
