package logrepository

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	pb "github.com/honnek/vigil/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type LogFilter struct {
	Service  string
	LevelMin pb.LogLevel
	Host     string
	From, To time.Time
	Text     string
	Limit    int
	Offset   int
}

type LogRepository interface {
	SaveBatch(ctx context.Context, entries []*pb.LogEntry) error
	Query(ctx context.Context, f LogFilter) ([]*pb.LogEntry, error)
}

type CHLogRepository struct {
	conn driver.Conn
}

func NewCHLogRepository(conn driver.Conn) *CHLogRepository {
	return &CHLogRepository{conn: conn}
}

func NewConnection(ctx context.Context, addr, db, user, pass string) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: db, Username: user, Password: pass},
	})
	if err != nil {
		return nil, err
	}
	err = conn.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (c *CHLogRepository) SaveBatch(ctx context.Context, entries []*pb.LogEntry) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO logs (timestamp, host, service, level, message, trace_id, fields)")
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := batch.Append(
			e.GetTimestamp().AsTime(),
			e.GetHost(),
			e.GetService(),
			int8(e.GetLevel()),
			e.GetMessage(),
			e.GetTraceId(),
			e.GetFields(),
		); err != nil {
			return err
		}
	}

	return batch.Send()
}

func (c *CHLogRepository) Query(ctx context.Context, f LogFilter) ([]*pb.LogEntry, error) {
	conds := make([]string, 0)
	args := make([]any, 0)

	if f.Service != "" {
		conds = append(conds, "service = ?")
		args = append(args, f.Service)
	}
	if f.Host != "" {
		conds = append(conds, "host = ?")
		args = append(args, f.Host)
	}
	if f.LevelMin != pb.LogLevel_LOG_LEVEL_UNSPECIFIED {
		conds = append(conds, "level >= ?")
		args = append(args, int8(f.LevelMin))
	}
	if !f.From.IsZero() {
		conds = append(conds, "timestamp >= ?")
		args = append(args, f.From)
	}
	if !f.To.IsZero() {
		conds = append(conds, "timestamp < ?")
		args = append(args, f.To)
	}
	if f.Text != "" {
		// hasToken использует tokenbf_v1-индекс; ищет целое слово, не подстроку
		conds = append(conds, "hasToken(message, ?)")
		args = append(args, f.Text)
	}

	// toInt8(level): Enum8 не сканится напрямую в *int8, приводим к числу (совпадает с pb.LogLevel)
	sql := "SELECT timestamp, host, service, toInt8(level) AS level, message, trace_id, fields FROM logs"
	if len(conds) > 0 {
		sql += " WHERE " + strings.Join(conds, " AND ")
	}
	sql += " ORDER BY timestamp DESC"

	limit := f.Limit
	if limit <= 0 {
		limit = 100 // защита от безлимитной выборки
	}
	sql += " LIMIT ?"
	args = append(args, limit)
	if f.Offset > 0 {
		sql += " OFFSET ?"
		args = append(args, f.Offset)
	}

	rows, err := c.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*pb.LogEntry, 0)

	for rows.Next() {
		var (
			ts                              time.Time
			host, service, message, traceID string
			level                           int8
			fields                          map[string]string
		)
		err := rows.Scan(&ts, &host, &service, &level, &message, &traceID, &fields)
		if err != nil {
			return nil, err
		}
		out = append(out, &pb.LogEntry{
			Host:      host,
			Timestamp: timestamppb.New(ts),
			Level:     pb.LogLevel(level),
			Service:   service,
			Message:   message,
			Fields:    fields,
			TraceId:   traceID,
		})
	}

	return out, rows.Err()
}
