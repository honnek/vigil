package logshipper

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"os/exec"
	"strconv"
	"time"

	pb "github.com/honnek/vigil/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Shipper struct {
	client pb.LogIngestClient
	host   string
}

// journalEntry — сырая запись journald из `journalctl -o json`.
// Все значения journald отдаёт строками (даже числа), поэтому Priority/RealtimeUsec — string.
type journalEntry struct {
	Message      string `json:"MESSAGE"`
	Priority     string `json:"PRIORITY"`          // syslog 0..7
	Unit         string `json:"_SYSTEMD_UNIT"`     // напр. "vigil-collector.service"
	Ident        string `json:"SYSLOG_IDENTIFIER"` // фолбэк, если Unit пуст
	Hostname     string `json:"_HOSTNAME"`
	RealtimeUsec string `json:"__REALTIME_TIMESTAMP"` // микросекунды с эпохи
	TraceID      string `json:"TRACE_ID"`             // best-effort, обычно пусто
}

func NewShipper(client pb.LogIngestClient, host string) *Shipper {
	return &Shipper{
		client: client,
		host:   host,
	}
}

func (s *Shipper) Run(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "journalctl", "-o", "json", "-f", "--since", "now")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Print(err)
		return nil
	}
	if err := cmd.Start(); err != nil {
		log.Print(err)
		return nil
	}

	stream, err := s.client.StreamLogs(ctx)
	if err != nil {
		log.Print(err)
		return nil
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		var logEntry journalEntry
		err := json.Unmarshal(line, &logEntry)
		if err != nil {
			log.Println(err)
			continue
		}

		entry := toLogEntry(logEntry, s.host)
		if entry == nil {
			continue
		}
		err = stream.Send(entry)
		if err != nil {
			log.Print(err)
		}
	}

	summary, err := stream.CloseAndRecv()
	if err != nil {
		log.Print(err)
	} else {
		log.Print(summary)
	}

	if err := scanner.Err(); err != nil {
		log.Println(err)
	}

	err = cmd.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func toLogEntry(je journalEntry, fallbackHost string) *pb.LogEntry {
	if je.Hostname == "" {
		je.Hostname = fallbackHost
	}
	service := je.Unit
	if service == "" {
		service = je.Ident
	}
	usec, err := strconv.ParseInt(je.RealtimeUsec, 10, 64)
	if err != nil {
		log.Println(err)
		return nil
	}
	pbEntry := pb.LogEntry{
		Host:      je.Hostname,
		Timestamp: timestamppb.New(time.UnixMicro(usec)),
		Level:     mapPriority(je.Priority),
		Service:   service,
		Message:   je.Message,
		Fields:    make(map[string]string),
		TraceId:   je.TraceID,
	}

	return &pbEntry
}

func mapPriority(p string) pb.LogLevel {
	switch p {
	case "0", "1", "2", "3":
		return pb.LogLevel_ERROR
	case "4":
		return pb.LogLevel_WARN
	case "5", "6":
		return pb.LogLevel_INFO
	case "7":
		return pb.LogLevel_DEBUG
	default:
		return pb.LogLevel_LOG_LEVEL_UNSPECIFIED
	}
}
