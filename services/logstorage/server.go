package main

import (
	"context"

	pb "github.com/honnek/vigil/proto"
	logrepository "github.com/honnek/vigil/services/logstorage/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LogsQueryService struct {
	pb.UnimplementedLogsQueryServer
	repo *logrepository.CHLogRepository
}

func NewLogsQueryService(repo *logrepository.CHLogRepository) *LogsQueryService {
	return &LogsQueryService{repo: repo}
}

func (s *LogsQueryService) QueryLogs(ctx context.Context, req *pb.LogQuery) (*pb.LogQueryResult, error) {
	f := logrepository.LogFilter{
		Service:  req.GetService(),
		LevelMin: req.GetLevelMin(),
		Host:     req.GetHost(),
		Text:     req.GetText(),
		Limit:    int(req.GetLimit()),
		Offset:   int(req.GetOffset()),
	}
	if req.GetFrom() != nil {
		f.From = req.GetFrom().AsTime()
	}
	if req.GetTo() != nil {
		f.To = req.GetTo().AsTime()
	}

	entries, err := s.repo.Query(ctx, f)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query logs: %v", err)
	}

	return &pb.LogQueryResult{
		Entries: entries,
		Total:   int64(len(entries)),
	}, nil

}
