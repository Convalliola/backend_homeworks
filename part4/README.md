# Part 4 — Асинхронная модерация объявлений

Асинхронная обработка объявлений через брокер сообщений Kafka

## Предварительные требования

- Python 3.12
- PostgreSQL (база `homework3` на `localhost:5432`)
- Docker и Docker Compose (для Kafka/Redpanda)

## Запуск инфраструктуры

### 1. Kafka (Redpanda)

```bash
docker compose up -d
```

Redpanda доступен на `localhost:9092`, консоль на `http://localhost:8080`

### 2. Миграции БД

```bash
pgmigrate -c "host=127.0.0.1 port=5432 dbname=homework3 user=radilkhanova password=postgres" -d db -t latest migrate
```

## Запуск приложения

### API-сервер

```bash
uvicorn main:app --reload
```

Сервер запустится на `http://localhost:8000`, документация `http://localhost:8000/docs`

### Воркер модерации (Kafka Consumer)

В отдельном терминале:

```bash
cd part4
python -m workers.moderation_worker
```

Воркер подписывается на топик `moderation`, обрабатывает сообщения и записывает результат в базу данных.

## API-эндпоинты

POST `/predict` - синхронное предсказание (данные в теле запроса)
POST `/simple_predict?item_id=N` - синхронное предсказание по id объявления из БД
POST `/async_predict?item_id=N` - асинхронная модерация: создаёт задачу и возвращает `task_id`
GET  `/moderation_result/{task_id}` - получение статуса/результата модерации

### Пример асинхронной модерации

```bash
# 1) создание задачи
curl  -w '\n' -X POST "http://localhost:8000/async_predict?item_id=1"
# {"task_id": 5, "status": "pending", "message": "Moderation request accepted"}

# 2) проверка статуса (polling)
curl  -w '\n' "http://localhost:8000/moderation_result/5"
# {"task_id": 5, "status": "completed", "is_violation": false, "probability": 0.85}
```

## Обработка ошибок

- Постоянные ошибки (объявление не найдено): статус `failed`, сообщение отправляется в DLQ топик `moderation_dlq`.
- Временные ошибки (ML-модель недоступна): до 3 повторных попыток с экспоненциальной задержкой (5с, 10с, 20с), после чего `failed` и DLQ.

## Тесты

```bash
python -m pytest tests/ -v
```
