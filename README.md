# Vigil — Distributed Observability Platform

> Платформа мониторинга инфраструктуры на Go. Агенты собирают метрики с хостов и стримят их на сервер по gRPC; бэкенд валидирует, хранит, агрегирует и рассылает алерты.

![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go)
![License](https://img.shields.io/badge/license-MIT-green)

Пет-проект: production-стек observability (gRPC + Kafka + PostgreSQL + Redis + Prometheus) и распределёнными системами. 
Мета-идея: система мониторинга мониторит сама себя.

---

## Архитектура

```
        Hosts
  ┌────────────┐  ┌────────────┐  ┌────────────┐
  │vigil-agent │  │vigil-agent │  │vigil-agent │  ...
  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
        └─────────── gRPC (streaming) ──┘
                       │
               ┌───────▼────────┐
               │vigil-collector │  валидация, нормализация
               └───────┬────────┘
                       │ Kafka: metrics.raw
          ┌────────────┼────────────┐
   ┌──────▼───────┐    │     ┌──────▼───────┐
   │vigil-processor│   │     │ vigil-alerter│
   │  агрегация   │    │     │  правила     │
   └──────┬───────┘    │     └──────┬───────┘
   ┌──────▼───────┐    │     ┌──────▼───────┐
   │vigil-storage │    │     │vigil-notifier│
   │Postgres+Redis│    │     │ Telegram/web │
   └──────┬───────┘    │     └──────────────┘
          │     ┌──────▼──────┐
          └────►│  vigil-api  │  REST + JWT
                └─────────────┘
```

---

## Технологии

| Слой        | Стек                                            |
| ----------- | ----------------------------------------------- |
| Транспорт   | gRPC + protobuf (streaming)                     |
| Event bus   | Apache Kafka                                     |
| Хранилище   | PostgreSQL (партиционирование по времени)        |
| Кэш         | Redis (горячие метрики)                          |
| Миграции    | goose (embed + CLI)                             |
| Драйвер БД  | pgx/v5 (pgxpool, CopyFrom)                       |
| Метрики     | Prometheus + Grafana                            |
| Логи        | slog                                            |
| Контейнеры  | Docker + docker-compose                         |

---

## Сервисы

| Сервис            | Порт  | Роль                                                       |
| ----------------- | ----- | ---------------------------------------------------------- |
| `vigil-agent`     | —     | Сбор метрик хоста (CPU, RAM, диск) через Strategy → gRPC   |
| `vigil-collector` | :9090 | Приём gRPC-потоков, валидация, publish в Kafka              |
| `vigil-storage`   | :9091 | gRPC API записи/чтения метрик, PostgreSQL + Redis          |
| `vigil-processor` | —     | Consume из Kafka, батч → storage, агрегация (скользящие средние) |
| `vigil-alerter`   | —     | Оценка правил, дедупликация + renotify, silence            |
| `vigil-notifier`  | —     | Доставка алертов в Telegram (консьюмер alerts + retry)     |
| `vigil-api`       | :8080 | REST Gateway (chi), JWT, /metrics, /alerts, swagger        |

---

## Структура

```
vigil/
├── proto/                  # protobuf-схемы и сгенерированный код
├── services/
│   ├── agent/              # сбор метрик (Strategy: CPU/RAM/Disk)
│   ├── collector/          # gRPC-сервер приёма + валидация + publish в Kafka
│   ├── processor/          # consume Kafka → storage, агрегация (worker pool)
│   ├── alerter/            # правила, дедуп/renotify, silence → топик alerts
│   └── storage/            # gRPC + PostgreSQL (миграции, repository)
│       ├── migrations/     # goose SQL-миграции
│       └── repository/     # pgx: SaveBatch / List / EnsurePartitions
├── pkg/
│   ├── kafka/              # продюсер + consumer group (sarama)
│   └── circuitbreaker/     # машина состояний Closed/Open/HalfOpen
├── docker-compose.yml
└── Makefile
```

---

## Быстрый старт

Требования: Go 1.25+, Docker, `protoc` (для регенерации proto).

```bash
# поднять весь стек (agent, collector, kafka, processor, alerter, storage, postgres, redis)
make up
```

Миграции БД storage накатывает сам при старте (goose, embed). Для ручного прогона:

```bash
make migrate-install
make migrate
```

Полезные make-таргеты:

| Команда                       | Действие                                  |
| ----------------------------- | ----------------------------------------- |
| `make up` / `make down`       | Поднять / остановить контейнеры           |
| `make proto`                  | Сгенерировать Go-код из `.proto`          |
| `make migrate`                | Накатить миграции                         |
| `make migrate-create name=x`  | Создать новую миграцию                    |
| `make migrate-status`         | Статус миграций                           |

---

## Лицензия

MIT
