package main

import (
	"encoding/json"
	"io"
	"log"
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
	logs      pb.LogsQueryClient
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

type SeriesDTO struct {
	Host string `json:"host"`
	Name string `json:"name"`
}

type LogDTO struct {
	TS      time.Time         `json:"ts"`
	Host    string            `json:"host"`
	Service string            `json:"service"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	TraceID string            `json:"trace_id,omitempty"`
	Fields  map[string]string `json:"fields,omitempty"`
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

// seriesHandler godoc
// @Summary  Список рядов (host + имя метрики) для пикеров
// @Tags     metrics
// @Produce  json
// @Param    since query    string false "показывать ряды, активные после этого момента, RFC3339 (по умолчанию now-1h)"
// @Success  200   {array}  SeriesDTO
// @Failure  400   {string} string "неверные параметры"
// @Failure  401   {string} string "нет/невалидный токен"
// @Failure  500   {string} string "ошибка storage"
// @Security BearerAuth
// @Router   /series [get]
func (h *APIHandler) seriesHandler(w http.ResponseWriter, r *http.Request) {
	since := time.Now().Add(-time.Hour)
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, "since must be in RFC3339 format", http.StatusBadRequest)
			return
		}
		since = t
	}

	resp, err := h.storage.ListSeries(r.Context(), &pb.ListSeriesRequest{
		Since: timestamppb.New(since),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	out := make([]SeriesDTO, 0, len(resp.GetSeries()))
	for _, s := range resp.GetSeries() {
		out = append(out, SeriesDTO{Host: s.GetHost(), Name: s.GetName()})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
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

// logsHandler godoc
// @Summary  Поиск логов с фильтрами
// @Tags     logs
// @Produce  json
// @Param    service query    string false "имя сервиса"
// @Param    host    query    string false "хост"
// @Param    level   query    string false "минимальный уровень: DEBUG/INFO/WARN/ERROR"
// @Param    text    query    string false "поиск по тексту сообщения (целое слово)"
// @Param    from    query    string false "начало периода, RFC3339"
// @Param    to      query    string false "конец периода, RFC3339"
// @Param    limit   query    int    false "макс. число записей (по умолчанию 100)"
// @Param    offset  query    int    false "смещение для пагинации"
// @Success  200     {array}  LogDTO
// @Failure  400     {string} string "неверные параметры"
// @Failure  401     {string} string "нет/невалидный токен"
// @Failure  500     {string} string "ошибка logstorage"
// @Security BearerAuth
// @Router   /logs [get]
func (h *APIHandler) logsHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	req := &pb.LogQuery{
		Service:  q.Get("service"),
		Host:     q.Get("host"),
		Text:     q.Get("text"),
		LevelMin: parseLevel(q.Get("level")),
	}
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, "from must be RFC3339", 400)
			return
		}
		req.From = timestamppb.New(t)
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, "to must be RFC3339", 400)
			return
		}
		req.To = timestamppb.New(t)
	}
	if v := q.Get("limit"); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil {
			http.Error(w, "limit must be an integer", 400)
			return
		}
		req.Limit = int64(limit)
	}
	if v := q.Get("offset"); v != "" {
		offset, err := strconv.Atoi(v)
		if err != nil {
			http.Error(w, "offset must be an integer", 400)
			return
		}
		req.Offset = int64(offset)
	}

	res, err := h.logs.QueryLogs(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	out := make([]LogDTO, 0, res.GetTotal())
	for _, e := range res.Entries {
		out = append(out, LogDTO{
			TS: e.GetTimestamp().AsTime(), Host: e.GetHost(), Service: e.GetService(),
			Level: e.GetLevel().String(), Message: e.GetMessage(),
			TraceID: e.GetTraceId(), Fields: e.GetFields(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(out)
	if err != nil {
		log.Println("Error encoding logs: ", err)
	}
}

func parseLevel(l string) pb.LogLevel {
	if l == "" {
		return pb.LogLevel_LOG_LEVEL_UNSPECIFIED
	}
	return pb.LogLevel(pb.LogLevel_value[strings.ToUpper(l)])
}
