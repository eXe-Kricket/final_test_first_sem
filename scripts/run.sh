#!/usr/bin/env bash

set -euo pipefail

echo "Запуск приложения в фоне..."
go run main.go &
SERVER_PID=$!

echo "Ожидание запуска сервера (15 секунд)..."
sleep 15

# Проверяем, что процесс ещё жив
if ! ps -p $SERVER_PID > /dev/null; then
  echo "Приложение упало во время запуска"
  exit 1
fi

# Проверяем, что сервер реально отвечает
if ! curl -s -f -o /dev/null http://localhost:8080/api/v0/prices; then
  echo "Сервер не отвечает после 15 секунд"
  kill $SERVER_PID 2>/dev/null
  exit 1
fi

echo "Smoke-тест успешно пройден"

# Останавливаем сервер
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null || true

exit 0
