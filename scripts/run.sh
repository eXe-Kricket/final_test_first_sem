#!/usr/bin/env bash

set -euo pipefail

echo "Запуск приложения в фоне..."
go run main.go &
SERVER_PID=$!

echo "Ожидание запуска сервера (максимум 60 секунд)..."

# Активное ожидание с таймаутом
for i in {1..60}; do
  if curl -s -f -o /dev/null http://localhost:8080/api/v0/prices; then
    echo "Сервер отвечает → smoke-тест прошёл"
    # Можно сделать реальный тест, если нужно
    curl -v http://localhost:8080/api/v0/prices
    break
  fi
  echo "Сервер ещё не готов (попытка $i/60)..."
  sleep 1
done

# Если после 60 сек не ответил — провал
if ! ps -p $SERVER_PID > /dev/null; then
  echo "Приложение упало во время запуска"
  exit 1
fi

if ! curl -s -f -o /dev/null http://localhost:8080/api/v0/prices; then
  echo "Сервер не запустился за 60 секунд"
  kill $SERVER_PID 2>/dev/null
  exit 1
fi

# Успех — останавливаем процесс
echo "Smoke-тест успешно пройден"
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null || true

exit 0