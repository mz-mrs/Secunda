# Payment Processing Service for Secunda (TestTask)

Асинхронный микросервис процессинга платежей на FastAPI.

Сервис принимает запрос на создание платежа, сохраняет его в PostgreSQL и асинхронно передаёт событие на обработку через RabbitMQ. Consumer эмулирует обработку платежа внешним платёжным шлюзом, обновляет статус платежа и отправляет результат клиенту через webhook.

Для обеспечения надёжности обработки используются:

Outbox Pattern — гарантированная публикация событий;
Idempotency-Key — защита от повторного создания платежа;
Retry — повторные попытки обработки временных ошибок;
Dead Letter Queue (DLQ) — для сообщений, которые не удалось обработать после 3 попыток.

## Стек

* Python 3.14
* FastAPI
* PostgreSQL 17
* SQLAlchemy 2.0
* Alembic
* RabbitMQ 4
* FastStream
* Poetry
* Docker / Docker Compose

## Архитектура

Основной поток обработки платежа:

```text
Client
  │
  │ POST /api/v1/payments
  │ X-API-Key
  │ Idempotency-Key
  ▼
FastAPI
  │
  ├──────────────► PostgreSQL
  │                 │
  │                 ├── payments
  │                 └── outbox
  │
  ▼
Publisher
  │
  ▼
RabbitMQ
  │
  ▼
payments.new
  │
  ▼
Consumer
  │
  ├──► Payment processing
  │
  ├──► Update payment status
  │
  └──► Webhook
          │
          ▼
       Client
```
При ошибках обработки используются повторные попытки. После трёх неуспешных попыток сообщение направляется в Dead Letter Queue.

## Быстрый запуск

### Требования

Для запуска необходимо установить:

* Docker
* Docker Compose

### Настройка окружения

В репозитории находится .env.example с тестовыми значениями конфигурации, необходимыми для локального запуска.
Создайте .env на его основе.

Для Linux/macOS:

```bash
cp .env.example .env
```

Для Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Файл .env не хранится в репозитории и добавлен в .gitignore.

### Запуск

Клонировать репозиторий:

```bash
git clone https://github.com/mz-mrs/Secunda
cd Secunda
```

Запустить все сервисы:

```bash
docker compose up --build
```

Docker Compose запускает:
- PostgreSQL;
- RabbitMQ;
- сервис миграций;
- API;
- Consumer.

После успешного выполнения миграций сервис migration завершает работу. Это ожидаемое поведение.

После запуска будут доступны:

* API: `http://localhost:8000`
* Swagger UI: `http://localhost:8000/docs`
* RabbitMQ Management: `http://localhost:15672`

Для остановки:

```bash
docker compose down
```

Для остановки с удалением данных PostgreSQL:

```bash
docker compose down -v
```

## API Authentication

Все API endpoints защищены статическим API-ключом.

Ключ передаётся в HTTP-заголовке:
```text
X-API-Key: <API_KEY>
```

Проверка выполняется на уровне HTTP-заголовков до обработки запроса.

Пример:

```bash
curl http://localhost:8000/api/v1/payments/{payment_id} \
  -H "X-API-Key: <API_KEY>"
```

При отсутствии API-ключа или передаче неверного значения запрос отклоняется.

API-ключ хранится в конфигурации приложения и не должен содержаться непосредственно в исходном коде.

## Idempotency

Для защиты от повторного создания платежа используется обязательный HTTP-заголовок:

```http
Idempotency-Key: <unique-key>
```

Один и тот же Idempotency-Key не может использоваться для создания нескольких платежей.

Пример:

```http
POST /api/v1/payments
X-API-Key: <API_KEY>
Idempotency-Key: order-123-payment
Content-Type: application/json
```

Повторная отправка запроса с тем же Idempotency-Key не создаёт новый платёж.

Идемпотентность обеспечивается на уровне базы данных с использованием уникального ключа.

## API

### Создание платежа

```http
POST /api/v1/payments
Content-Type: application/json
```
Обязательные заголовки:
```http
X-API-Key: <API_KEY>
Idempotency-Key: order-123-payment
Content-Type: application/json
```
Тело запроса:
```json
{
  "amount": 1000.00,
  "currency": "RUB",
  "description": "Test payment",
  "payment_metadata": {
    "order_id": "123"
  },
  "webhook_url": "http://localhost:8000/test-webhook"
}
```

Поддерживаемые валюты:
- RUB
- USD
- EUR

Пример через curl:
```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <API_KEY>" \
  -H "Idempotency-Key: order-123-payment" \
  -d '{
    "amount": 1000.00,
    "currency": "RUB",
    "description": "Test payment",
    "payment_metadata": {
      "order_id": "123"
    },
    "webhook_url": "http://localhost:8000/test-webhook"
  }'
```
Ответ:
```http
HTTP/1.1 202 Accepted
{
  "payment_id": "7b3b5b6f-7f6d-4f9d-9a4a-123456789abc",
  "status": "pending",
  "created_at": "2026-08-21T10:00:00Z"
}
```
После получения ответа платеж продолжает обрабатываться асинхронно.

### Получение платежа

```http
GET /api/v1/payments/{payment_id}
```

Пример:

```bash
curl http://localhost:8000/api/v1/payments/7b3b5b6f-7f6d-4f9d-9a4a-123456789abc
```

Пример ответа:
```json
{
  "id": "7b3b5b6f-7f6d-4f9d-9a4a-123456789abc",
  "amount": 1000.00,
  "currency": "RUB",
  "description": "Test payment",
  "payment_metadata": {
    "order_id": "123"
  },
  "status": "succeeded",
  "webhook_url": "http://localhost:8000/test-webhook",
  "created_at": "2026-08-21T10:00:00Z",
  "processed_at": "2026-08-21T10:00:04Z"
}
```
Возможные статусы:
- pending
- succeeded
- failed

## Асинхронная обработка

После создания платежа событие сохраняется в Outbox и затем публикуется в RabbitMQ.

Используются очереди:

```text
payments.new
payments.retry
payments.dlq
```

Основной consumer получает сообщения из `payments.new`.

Если обработка завершается ошибкой, выполняются повторные попытки. После исчерпания допустимого количества попыток сообщение попадает в `payments.dlq`.

## Consumer

Consumer получает сообщения из payments.new и выполняет весь цикл обработки:

1. получает сообщение;
2. эмулирует обращение к платёжному шлюзу;
3. ожидает 2–5 секунд;
4. получает результат обработки;
5. обновляет статус платежа в PostgreSQL;
6. отправляет webhook на указанный webhook_url.

Эмуляция платёжного шлюза использует вероятность:

90% — успешная обработка
10% — ошибка

## Webhook

После обработки платежа consumer отправляет уведомление на webhook_url, указанный при создании платежа.

Webhook содержит информацию о результате обработки платежа.

Для локального тестирования в приложении предусмотрен endpoint:
```http
GET /test-webhook
```
Проверка:

```bash
curl http://localhost:8000/test-webhook
```

Пример ответа:
```json
{
  "status": "webhook received successfully"
}
```

## Retry

При ошибке отправки webhook выполняются повторные попытки.

Всего выполняется до 3 попыток с экспоненциальной задержкой между ними.

Схематично:
```text
Attempt 1
   │
   └── error
        │
        ▼
      retry
        │
        ▼
Attempt 2
   │
   └── error
        │
        ▼
      retry
        │
        ▼
Attempt 3
   │
   ├── success → обработка завершена
   │
   └── error → DLQ
```
Сообщения, которые не удалось обработать после трёх попыток, направляются в payments.dlq.

## Dead Letter Queue

Для окончательно неуспешных сообщений используется Dead Letter Queue:

`payments.dlq`

DLQ позволяет сохранить сообщения, которые не удалось обработать после максимального количества попыток, вместо их бесконечной повторной обработки.

RabbitMQ Management позволяет проверить содержимое очереди и количество сообщений.

## Outbox Pattern

Для гарантированной публикации событий используется Outbox Pattern.

Создание платежа и соответствующая запись в Outbox сохраняются в одной транзакции PostgreSQL:
```text
Transaction
    │
    ├── Payment
    │
    └── Outbox event
```
Это предотвращает ситуацию, при которой платеж сохранён в базе данных, но событие для RabbitMQ потеряно.

После успешного commit отдельный процесс публикует события из Outbox в RabbitMQ.

Для конкурентной обработки Outbox используется блокировка:

`FOR UPDATE SKIP LOCKED`

## Миграции базы данных

При запуске Docker Compose автоматически запускается отдельный сервис migration.

Он применяет актуальные миграции базы данных с помощью Alembic:
```text
migration
    │
    └── alembic upgrade head
            │
            └── завершение контейнера
```
Сервис migration является одноразовым: после успешного применения миграций контейнер завершается. Это ожидаемое поведение.

Основные таблицы:
```text
payments
outbox
```
## Health Check

Проверить состояние API:

```bash
curl http://localhost:8000/health
```

Пример ответа:

```json
{
  "status": "bingo!"
}
```

## Проверка RabbitMQ

RabbitMQ Management доступен по адресу:

```http
http://localhost:15672
```


В интерфейсе RabbitMQ можно проверить:

* очереди `payments.new`, `payments.retry` и `payments.dlq`;
* количество сообщений;
* публикацию и обработку событий.



## Структура проекта

```text
SecundaTest/
    ├── migrations/             # Database migrations
    ├── secundatest/            # Application folder
            ├── api/            # REST API & dependencies
            ├── broker/         # RabbitMQ publisher/consumer
            ├── core/           # Config file & logger
            ├── db/             # Database session
            ├── enums/          # Payment and Outbox statuses
            ├── models/         # Payment and Outbox models
            ├── schemas/        # Request/response schemas
            ├── services/       # Business logic
            ├── main.py         # Application entry point
    ├── Dockerfile
    ├── docker-compose.yml
    ├── pyproject.toml
    └── .env.example
```

## Основные сценарии

Успешная обработка
```text
Client
  │
  │ POST /api/v1/payments
  ▼
FastAPI
  │
  ├── Payment ──────► PostgreSQL
  │
  └── Outbox ───────► PostgreSQL
                         │
                         ▼
                    RabbitMQ
                         │
                         ▼
                  payments.new
                         │
                         ▼
                     Consumer
                         │
                         ├── Payment processing
                         │
                         ├── status = succeeded
                         │
                         └── Webhook
                              │
                              ▼
                            Client
```
Обработка ошибки
```text
Consumer
   │
   ▼
Processing error
   │
   ▼
Retry
   │
   ├── attempt 1
   ├── attempt 2
   └── attempt 3
          │
          ▼
      payments.dlq
```

## Примечание

Проект выполнен в рамках тестового задания и демонстрирует реализацию сервиса обработки платежей с использованием REST API, PostgreSQL и RabbitMQ.

Основное внимание уделено надёжности обработки сообщений, идемпотентности операций и гарантированной публикации событий.

## Автор

Андреева Марина
GitHub: https://github.com/mz-mrs
Telegram: @mz_mrs