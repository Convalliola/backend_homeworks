# Сервис модерации объявлений

Асинхронный сервис модерации объявлений с ML-моделью, JWT-аутентификацией, кэшированием Redis и брокером сообщений Kafka.

## Предварительные требования

- Python 3.12+
- Docker и Docker Compose

## Архитектура

```
Routes -> Services -> Repositories -> Storages -> PostgreSQL / Redis
```

- **Storages** — работа с конкретным хранилищем (PG SQL-запросы, управление соединениями)
- **Repositories** — абстракция поверх стораджей (PG + Redis-кэш)
- **Services** — бизнес-логика (авторизация, предсказания)
- **Routes** — HTTP-эндпоинты FastAPI

## Запуск инфраструктуры

```bash
docker compose up -d
```

Сервисы:

| Сервис     | Адрес              |
|------------|--------------------|
| Kafka (Redpanda) | `localhost:9092` |
| Kafka Console    | `http://localhost:8080` |
| PostgreSQL       | `localhost:5435` (db: `hw`, user/pass: `postgres`) |
| Redis            | `localhost:6379` |
| MLflow           | `http://localhost:5000` (UI и tracking API) |
| Prometheus       | `http://localhost:9090` |
| Grafana          | `http://localhost:3000` (admin/admin) |

## Миграции БД

```bash
pgmigrate -c "host=127.0.0.1 port=5435 dbname=hw user=$PG_USER password=$PG_PASSWORD" -d db -t latest migrate
```

## Переменные окружения

Скопируйте `.env.example` и заполните значения. Основные переменные:

| Переменная | Описание | По умолчанию |
|---|---|---|
| `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE` | PostgreSQL | `127.0.0.1:5435`, `hw` |
| `REDIS_HOST`, `REDIS_PORT` | Redis | `localhost:6379` |
| `JWT_SECRET` | Секрет для подписи JWT | `super-secret-key-change-me` |
| `USE_MLFLOW` | Загружать модель из MLflow (`true`/`false`) | `false` |
| `MLFLOW_TRACKING_URI` | URI трекинг-сервера MLflow | `sqlite:///mlflow.db` локально или `http://127.0.0.1:5000` при MLflow из compose |
| `MLFLOW_MODEL_NAME` | Имя модели в MLflow Registry | `moderation-model` |
| `MLFLOW_MODEL_STAGE` | Стадия модели | `Production` |
| `SENTRY_DSN` | DSN для Sentry (опционально) | — |

## Запуск приложения

### API-сервер

```bash
uvicorn main:app --reload
```

Сервер: `http://localhost:8000`, документация: `http://localhost:8000/docs`

### Воркер модерации (Kafka Consumer)

```bash
python -m workers.moderation_worker
```

Подписывается на топик `moderation`, обрабатывает задачи и записывает результат в БД.

## ML-модель

По умолчанию (`USE_MLFLOW=false`) модель загружается из локального файла `model.pkl`. Если файла нет — обучается автоматически (LogisticRegression на синтетических данных).

### Загрузка из MLflow

**Вариант A — MLflow из Docker Compose** (рекомендуется для единой инфраструктуры):

1. Поднимите стек: `docker compose up -d` (сервис `mlflow` на порту `5000`).
2. Укажите трекинг URI и зарегистрируйте модель в том же хранилище, что и сервер:

```bash
export MLFLOW_TRACKING_URI=http://127.0.0.1:5000
python register_model.py
```

3. Переведите модель в стадию **Production** (MLflow UI `http://localhost:5000` или CLI).

4. Запустите приложение с:

```bash
export USE_MLFLOW=true
export MLFLOW_TRACKING_URI=http://127.0.0.1:5000
uvicorn main:app --reload
```

**Вариант B — локальный SQLite без контейнера** (файл `mlflow.db` в каталоге проекта):

1. `python register_model.py` (по умолчанию `MLFLOW_TRACKING_URI` указывает на локальный файл).
2. Переведите модель в Production.
3. `export USE_MLFLOW=true` и запуск приложения.

> Воркер и API должны использовать тот же `MLFLOW_TRACKING_URI`, что и при регистрации модели.

## API-эндпоинты

Все эндпоинты кроме `/login` и `/metrics` требуют JWT-аутентификации (cookie `access_token`).

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| POST | `/login` | Авторизация, возвращает JWT в cookie |
| POST | `/predict` | Синхронное предсказание (данные в теле запроса) |
| POST | `/simple_predict?item_id=N` | Синхронное предсказание по id объявления из БД |
| POST | `/async_predict?item_id=N` | Асинхронная модерация, возвращает `task_id` |
| GET | `/moderation_result/{task_id}` | Статус/результат модерации |
| POST | `/close?item_id=N` | Закрытие объявления, инвалидация кэша |
| GET | `/metrics` | Метрики Prometheus |

### Поведение `POST /close`

Объявление **не удаляется** из таблицы `ads`. Выполняется **мягкое закрытие**: в строке выставляется `is_closed = TRUE` (объявление больше не участвует в выдаче/модерации в рамках текущей логики). После этого удаляются связанные записи модерации и инвалидируются ключи в Redis (`predict:item`, `moderation:result` для затронутых задач). Физическое удаление строки объявления (`DELETE`) в этом эндпоинте не используется.

### Пример использования

```bash
# авторизация
curl -c cookies.txt -X POST http://localhost:8000/login \
  -H "Content-Type: application/json" \
  -d '{"login": "alice", "password": "secret"}'

# синхронное предсказание
curl -b cookies.txt -X POST "http://localhost:8000/simple_predict?item_id=1"

# асинхронная модерация
curl -b cookies.txt -X POST "http://localhost:8000/async_predict?item_id=1"
# {"task_id": 5, "status": "pending", "message": "Moderation request accepted"}

# проверка статуса
curl -b cookies.txt "http://localhost:8000/moderation_result/5"
# {"task_id": 5, "status": "completed", "is_violation": false, "probability": 0.85}
```

## Кэширование (Redis)

| Ключ | TTL | Описание |
|------|-----|----------|
| `predict:item:{id}` | 10 мин | Предсказание по id объявления |
| `predict:features:...` | 1 час | Предсказание по фичам |
| `moderation:result:{id}` | 30 мин | Результат модерации (completed/failed) |
| `account:{id}` | 5 мин | Аккаунт по id (аутентификация) |

## Обработка ошибок

- **Постоянные ошибки** (объявление не найдено): статус `failed`, сообщение в DLQ (`moderation_dlq`).
- **Временные ошибки** (ML-модель недоступна): до 3 попыток с экспоненциальной задержкой (5с, 10с, 20с), затем `failed` и DLQ.
- **Sentry**: исключения автоматически отправляются в Sentry (если настроен `SENTRY_DSN`).

## Мониторинг

- **Prometheus**: HTTP-метрики (`http_requests_total`, `http_request_duration_seconds`), метрики предсказаний, метрики БД.
- **Grafana**: `http://localhost:3000` (admin/admin), источник данных — Prometheus.

## Тесты

```bash
# юнит-тесты (без внешних зависимостей)
python -m pytest tests/ -v --ignore=tests/test_account_storage.py --ignore=tests/test_pg_repositories.py

# интеграционные тесты (требуют PostgreSQL)
python -m pytest tests/ -v -m integration
```
