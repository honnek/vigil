.PHONY: proto-install proto up down

# Установка плагинов-генераторов (один раз)
proto-install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Генерация Go-кода из .proto
proto:
	protoc -I proto \
		--go_out=proto --go_opt=paths=source_relative \
		--go-grpc_out=proto --go-grpc_opt=paths=source_relative \
		metric.proto storage.proto alert.proto

# Поднять все контейнеры (со сборкой образов)
up:
	docker compose up -d

build:
	docker compose up -d --build

# Остановить и удалить контейнеры
down:
	docker compose down


  # DSN для локального постгреса из docker-compose
DSN := postgres://vigil:secret@localhost:5432/vigil?sslmode=disable
MIGRATIONS_DIR := services/storage/migrations

  # Установка goose CLI (один раз)
migrate-install:
	go install github.com/pressly/goose/v3/cmd/goose@latest

  # Создать новую миграцию: make migrate-create name=add_something
migrate-create:
	goose -dir $(MIGRATIONS_DIR) create $(name) sql

  # Накатить все pending миграции
migrate:
	goose -dir $(MIGRATIONS_DIR) postgres "$(DSN)" up

  # Откатить последнюю
migrate-down:
	goose -dir $(MIGRATIONS_DIR) postgres "$(DSN)" down

  # Статус миграций
migrate-status:
	goose -dir $(MIGRATIONS_DIR) postgres "$(DSN)" status
