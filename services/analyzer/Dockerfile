FROM golang:1.25 AS builder
LABEL author="honnek"
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app/bin ./services/analyzer

FROM alpine:3.20
COPY --from=builder /app/bin /app/bin
EXPOSE 2112
ENTRYPOINT ["/app/bin"]
