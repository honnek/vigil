package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/honnek/vigil/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MetricFilter struct {
	Host  string
	Name  string
	From  time.Time
	To    time.Time
	Limit int
}

type MetricRepository interface {
	SaveBatch(ctx context.Context, metrics []*pb.Metric) error
	List(ctx context.Context, f MetricFilter) ([]*pb.Metric, error)
}

type PgMetricRepository struct {
	pool *pgxpool.Pool
}

func NewPgMetricRepository(pool *pgxpool.Pool) *PgMetricRepository {
	return &PgMetricRepository{pool: pool}
}

func (r *PgMetricRepository) SaveBatch(ctx context.Context, metrics []*pb.Metric) error {
	rows := make([][]any, 0, len(metrics))

	for _, metric := range metrics {
		labels, err := json.Marshal(metric.Labels)
		if err != nil {
			return err
		}

		rows = append(rows, []any{
			metric.GetHost(),
			metric.GetName(),
			int16(metric.GetType()),
			metric.GetValue(),
			labels,
			metric.GetTimestamp().AsTime(),
		})
	}

	_, err := r.pool.CopyFrom(
		ctx,
		pgx.Identifier{"metrics"},
		[]string{"host", "name", "type", "value", "labels", "ts"},
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		return fmt.Errorf("pgMetricRepository.SaveBatch: %w", err)
	}

	return nil
}

func (r *PgMetricRepository) List(ctx context.Context, f MetricFilter) ([]*pb.Metric, error) {
	sql := `SELECT host, name, type, value, labels, ts 
			FROM metrics
			WHERE host = $1 AND name = $2 AND ts >= $3 AND ts < $4 
			ORDER BY ts DESC 
			LIMIT $5`

	rows, err := r.pool.Query(ctx, sql, f.Host, f.Name, f.From, f.To, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("pgMetricRepository.List: %w", err)
	}

	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*pb.Metric, error) {
		var (
			host, name string
			mType      int16
			value      float64
			labelsRaw  []byte
			ts         time.Time
		)
		if err := row.Scan(&host, &name, &mType, &value, &labelsRaw, &ts); err != nil {
			return nil, err
		}

		var labels map[string]string
		if len(labelsRaw) > 0 {
			if err := json.Unmarshal(labelsRaw, &labels); err != nil {
				return nil, err
			}
		}

		return &pb.Metric{
			Host:      host,
			Timestamp: timestamppb.New(ts),
			Type:      pb.MetricType(mType),
			Value:     value,
			Labels:    labels,
			Name:      name,
		}, nil
	})
}

func (r *PgMetricRepository) EnsurePartitions(ctx context.Context, ahead int) error {
	now := time.Now().UTC()
	base := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i <= ahead; i++ {
		from := base.AddDate(0, i, 0)
		to := base.AddDate(0, i+1, 0)
		name := fmt.Sprintf("metrics_%d_%02d", from.Year(), int(from.Month()))

		sql := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF metrics
       		FOR VALUES FROM ('%s') TO ('%s')`,
			name, from.Format("2006-01-02"), to.Format("2006-01-02"))

		_, err := r.pool.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("pgMetricRepository.EnsurePartitions: %w", err)
		}
	}

	return nil
}

func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	err = pool.Ping(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}
