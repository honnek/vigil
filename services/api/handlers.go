package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
)
import pb "github.com/honnek/vigil/proto"

type APIHandler struct {
	storage   pb.StorageServiceClient
	rdb       *redis.Client
	jwtSecret []byte
	user      string
	password  string
}

type MetricDTO struct {
	Host   string            `json:"host"`
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels"`
	TS     time.Time         `json:"ts"`
}

type AlertDTO struct {
	Host string `json:"host"`
	Rule string `json:"rule"`
}

// metricsHandler godoc
// @Summary  Список метрик за период
// @Tags     metrics
// @Produce  json
// @Param    host  query    string true  "хост"
// @Param    name  query    string true  "имя метрики (напр. cpu_usage_percent)"
// @Param    from  query    string true  "начало периода, RFC3339"
// @Param    to    query    string true  "конец периода, RFC3339"
// @Param    limit query    int    true  "макс. число точек"
// @Success  200   {array}  MetricDTO
// @Failure  400   {string} string "неверные параметры"
// @Failure  401   {string} string "нет/невалидный токен"
// @Failure  500   {string} string "ошибка storage"
// @Security BearerAuth
// @Router   /metrics [get]
func (h *APIHandler) metricsHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	host := q.Get("host")
	name := q.Get("name")
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil {
		http.Error(w, "limit must be an integer", http.StatusBadRequest)
		return
	}
	from, err := time.Parse(time.RFC3339, q.Get("from"))
	if err != nil {
		http.Error(w, "from must be in RFC3339 format", http.StatusBadRequest)
		return
	}
	to, err := time.Parse(time.RFC3339, q.Get("to"))
	if err != nil {
		http.Error(w, "to must be in RFC3339 format", http.StatusBadRequest)
		return
	}

	req := &pb.ListMetricsRequest{
		Host:  host,
		Name:  name,
		Limit: int64(limit),
		To:    timestamppb.New(to),
		From:  timestamppb.New(from),
	}

	stream, err := h.storage.ListMetrics(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var out []MetricDTO

	for {
		metric, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		out = append(out, MetricDTO{
			Host: metric.GetHost(), Name: metric.GetName(), Value: metric.GetValue(),
			Labels: metric.GetLabels(), TS: metric.GetTimestamp().AsTime(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(out)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// loginHandler godoc
// @Summary  Логин, выдаёт JWT-токен
// @Tags     auth
// @Accept   json
// @Produce  json
// @Param    credentials body     object true "логин и пароль" example({"username":"admin","password":"changeme"})
// @Success  200         {object} map[string]string "поле signed — JWT-токен"
// @Failure  400         {string} string "неверное тело запроса"
// @Failure  401         {string} string "неверные учётные данные"
// @Router   /login [post]
func (h *APIHandler) loginHandler(w http.ResponseWriter, r *http.Request) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if creds.Username != h.user || creds.Password != h.password {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}

	claims := jwt.MapClaims{
		"sub": creds.Username,
		"exp": time.Now().Add(time.Hour * 72).Unix(),
		"iat": time.Now().Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(h.jwtSecret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(map[string]string{"signed": signed})
}

// alertsHandler godoc
// @Summary  Текущие горящие алерты
// @Tags     alerts
// @Produce  json
// @Success  200 {array}  AlertDTO
// @Failure  401 {string} string "нет/невалидный токен"
// @Failure  500 {string} string "ошибка redis"
// @Security BearerAuth
// @Router   /alerts [get]
func (h *APIHandler) alertsHandler(w http.ResponseWriter, r *http.Request) {
	iter := h.rdb.Scan(r.Context(), 0, "alert:active:*", 0).Iterator()
	var out []AlertDTO

	for iter.Next(r.Context()) {
		key := iter.Val()
		parts := strings.SplitN(key, ":", 4)
		if len(parts) != 4 {
			continue
		}
		out = append(out, AlertDTO{
			Host: parts[2],
			Rule: parts[3],
		})
	}

	if err := iter.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
