package main

import (
	"io"
	"time"

	pb "github.com/honnek/vigil/proto"
	"github.com/honnek/vigil/services/storage/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StorageService struct {
	pb.UnimplementedStorageServiceServer
	repo repository.MetricRepository
}

func NewStorageService(repo repository.MetricRepository) *StorageService {
	return &StorageService{repo: repo}
}

func (s *StorageService) ListMetrics(req *pb.ListMetricsRequest, stream pb.StorageService_ListMetricsServer) error {
	ctx := stream.Context()
	if req.GetFrom() == nil || req.GetTo() == nil {
		return status.Errorf(codes.InvalidArgument, "From and To required")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 1000
	}

	f := repository.MetricFilter{
		Host:  req.Host,
		Name:  req.Name,
		From:  req.From.AsTime(),
		To:    req.To.AsTime(),
		Limit: limit,
	}

	metrics, err := s.repo.List(ctx, f)
	if err != nil {
		return err
	}

	for _, m := range metrics {
		if err := stream.Send(m); err != nil {
			return err
		}
	}

	return nil
}

func (s *StorageService) SaveMetrics(stream pb.StorageService_SaveMetricsServer) error {
	recvErr := make(chan error, 1)
	metricsCh := make(chan *pb.Metric)
	const maxBatchSize = 100
	buf := make([]*pb.Metric, 0, maxBatchSize)

	ctx := stream.Context()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	saved := 0

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := s.repo.SaveBatch(ctx, buf); err != nil {
			return err
		}

		saved += len(buf)
		buf = buf[:0]
		return nil
	}

	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}

			select {
			case metricsCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case m := <-metricsCh:
			buf = append(buf, m)
			if len(buf) >= maxBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}

		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}

		case err := <-recvErr:
			if err != io.EOF {
				return err
			}
			if err := flush(); err != nil {
				return err
			}

			return stream.SendAndClose(&pb.SaveSummary{
				Failed: 0,
				Saved:  int64(saved),
			})

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
