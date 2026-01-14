#!/usr/bin/env bash

set -euo pipefail

echo "Запуск приложения в фоне..."
go run main.go &
SERVER_PID=$!

# Даём серверу 10–15 секунд на запуск
sleep 12

echo "Smoke-тест: GET /api/v0/prices"
curl -v --fail http://localhost:8080/api/v0/prices -o /dev/null || {
  echo "Smoke-тест провален"
  kill $SERVER_PID
  exit 1
}

echo "Сервер отвечает → smoke-тест прошёл"

# Завершаем процесс (чтобы шаг CI завершился)
kill $SERVER_PID
wait $SERVER_PID || true

echo "Приложение успешно протестировано и остановлено"