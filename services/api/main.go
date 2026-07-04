package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	pb "github.com/honnek/vigil/proto"
	_ "github.com/honnek/vigil/services/api/docs"
	"github.com/redis/go-redis/v9"
	httpSwagger "github.com/swaggo/http-swagger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// @title       Vigil API
// @version     1.0
// @description REST-фасад Vigil: метрики из storage и текущие алерты. Защищено JWT.
// @host        localhost:8080
// @BasePath    /
// @securityDefinitions.apikey BearerAuth
// @in          header
// @name        Authorization
func main() {
	storageAddr := os.Getenv("STORAGE_ADDR")
	if storageAddr == "" {
		storageAddr = "localhost:9091"
	}
	apiAddr := os.Getenv("API_ADDR")
	if apiAddr == "" {
		apiAddr = ":8080"
	}
	jwt := os.Getenv("JWT_SECRET")
	if jwt == "" {
		log.Fatal("JWT_SECRET environment variable not set")
	}
	password := os.Getenv("PASSWORD")
	if password == "" {
		log.Fatal("PASSWORD environment variable not set")
	}
	user := os.Getenv("API_USER")
	if user == "" {
		log.Fatal("USER environment variable not set")
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(storageAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}

	hApi := APIHandler{
		storage:   pb.NewStorageServiceClient(conn),
		rdb:       redisClient,
		jwtSecret: []byte(jwt),
		user:      user,
		password:  password,
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger, middleware.Recoverer)

	r.Get("/swagger/*", httpSwagger.WrapHandler)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	r.Post("/login", hApi.loginHandler)

	r.Group(func(r chi.Router) {
		r.Use(authMiddleware([]byte(jwt)))
		r.Get("/metrics", hApi.metricsHandler)
		r.Get("/alerts", hApi.alertsHandler)
	})

	server := http.Server{Addr: apiAddr, Handler: r}

	go func() {
		err = server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	log.Printf("Listening on %s", apiAddr)

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	server.Shutdown(shutdownCtx)
}
