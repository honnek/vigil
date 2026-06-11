package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/honnek/vigil/proto"
	"github.com/honnek/vigil/services/storage/repository"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://vigil:secret@localhost:5432/vigil?sslmode=disable"
		log.Println("not found POSTGRES_DSN")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := RunMigrations(ctx, dsn); err != nil {
		log.Fatal(err)
	}

	pool, err := repository.NewPool(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	repo := repository.NewPgMetricRepository(pool)
	if err := repo.EnsurePartitions(ctx, 2); err != nil {
		log.Fatal(err)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}

	cachingRepo := repository.NewCachingRepository(repo, redisClient, 30*time.Second)

	s := grpc.NewServer()
	pb.RegisterStorageServiceServer(s, NewStorageService(cachingRepo))
	l, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := s.Serve(l); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("serving on port 9091")

	<-ctx.Done()
	s.GracefulStop()
}
