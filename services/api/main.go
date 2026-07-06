package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/honnek/vigil/pkg/metrics"
	pb "github.com/honnek/vigil/proto"
	_ "github.com/honnek/vigil/services/api/docs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	httpSwagger "github.com/swaggo/http-swagger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vigil_api_http_requests_total",
		Help: "Количество принятых запросов",
	}, []string{"method", "route", "status"})
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "vigil_api_http_request_duration_seconds",
		Help: "задержка",
	}, []string{"method", "route"})
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
	prometheusAddr := os.Getenv("METRICS_ADDR")
	if prometheusAddr == "" {
		prometheusAddr = ":2112"
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
	r.Use(middleware.Logger, middleware.Recoverer, prometheusMiddleware)

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

	metrics.Serve(prometheusAddr)

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	server.Shutdown(shutdownCtx)
}

func prometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		duration := time.Since(start).Seconds()
		route := chi.RouteContext(r.Context()).RoutePattern()
		status := strconv.Itoa(ww.Status())

		httpDuration.WithLabelValues(r.Method, route).Observe(duration)
		httpRequests.WithLabelValues(r.Method, route, status).Inc()
	})
}
