# ── Стадия 1: сборка ──
# Образ с Go-тулчейном компилирует исходники в статический бинарь.
# Целиком отбрасывается после сборки, в финальный образ не попадает.
FROM golang:1.25 AS builder
LABEL author="honnek"
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app/bin ./services/collector

# ── Стадия 2: рантайм ──
# Лёгкий образ только с готовым бинарём — без Go-тулчейна и исходников.
# Именно он становится финальным образом, который запускается в проде.
FROM alpine:3.20
COPY --from=builder /app/bin /app/bin
EXPOSE 9090
ENTRYPOINT ["/app/bin"]
