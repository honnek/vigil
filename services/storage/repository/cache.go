package repository

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	pb "github.com/honnek/vigil/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

var _ MetricRepository = (*CachingRepository)(nil)
var _ MetricRepository = (*PgMetricRepository)(nil)
var (
	metricsRedisReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_storage_metrics_received_total",
		Help: "Количество полученых редисом метрик",
	})
	metricsRedisRejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vigil_storage_metrics_rejected_total",
		Help: "Количество отклоненных редисом метрик",
	})
)

type CachingRepository struct {
	next      MetricRepository
	rdb       *redis.Client
	hotWindow time.Duration
}

func cacheKey(host, name string) string {
	return fmt.Sprintf("metrics:%s:%s", host, name)
}

func NewCachingRepository(next MetricRepository, rdb *redis.Client, hotWindow time.Duration) *CachingRepository {
	return &CachingRepository{
		next:      next,
		hotWindow: hotWindow,
		rdb:       rdb,
	}
}

func (r *CachingRepository) SaveBatch(ctx context.Context, metrics []*pb.Metric) error {
	if err := r.next.SaveBatch(ctx, metrics); err != nil {
		return err
	}

	pipe := r.rdb.Pipeline()

	for _, m := range metrics {
		data, err := proto.Marshal(m)
		if err != nil {
			return err
		}

		key := cacheKey(m.GetHost(), m.GetName())
		score := float64(m.GetTimestamp().AsTime().UnixMilli())
		pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: data})

		cutoff := float64(time.Now().Add(-r.hotWindow).UnixMilli())
		pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("(%f", cutoff))
		pipe.Expire(ctx, key, r.hotWindow)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("cache save batch error: %v", err)
	}

	return nil
}

func (r *CachingRepository) List(ctx context.Context, f MetricFilter) ([]*pb.Metric, error) {
	hotStart := time.Now().Add(-r.hotWindow)

	if !f.From.Before(hotStart) {
		metrics, err := r.listFromCache(ctx, f)
		if err == nil && len(metrics) > 0 {
			return metrics, nil
		}
	}

	return r.next.List(ctx, f)
}

func (r *CachingRepository) listFromCache(ctx context.Context, f MetricFilter) ([]*pb.Metric, error) {
	key := cacheKey(f.Host, f.Name)

	members, err := r.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     key,
		Start:   "(" + strconv.FormatInt(f.To.UnixMilli(), 10),
		Stop:    strconv.FormatInt(f.From.UnixMilli(), 10),
		ByScore: true,
		Rev:     true,
		Offset:  0,
		Count:   int64(f.Limit),
	}).Result()
	if err != nil {
		metricsRedisRejected.Inc()
		return nil, err
	}

	metrics := make([]*pb.Metric, 0, len(members))
	for _, member := range members {
		var m pb.Metric
		if err := proto.Unmarshal([]byte(member), &m); err != nil {
			metricsRedisRejected.Inc()
			return nil, err
		}
		metrics = append(metrics, &m)
	}

	metricsRedisReceived.Add(float64(len(members)))
	return metrics, nil
}
