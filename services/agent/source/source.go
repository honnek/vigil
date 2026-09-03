package source

import (
	"log"
	"os"
	"strconv"
	"time"

	pb "github.com/honnek/vigil/proto"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var host, _ = os.Hostname()

type Source interface {
	Collect() ([]*pb.Metric, error)
}

type CPUSource struct {
	// PerCoreEvery — как часто отдавать метрики по отдельным ядрам.
	// Нулевое значение означает «на каждый Collect».
	PerCoreEvery time.Duration
	lastPerCore  time.Time
}

type DiskIOSource struct {
	prevWrite uint64
	prevTime  time.Time
}

func (s *CPUSource) Collect() ([]*pb.Metric, error) {
	var metrics []*pb.Metric

	total, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}
	if len(total) > 0 {
		metrics = append(metrics, newMetric("cpu_total_percent", total[0], pb.MetricType_METRIC_TYPE_CPU))
	}

	now := time.Now()
	if now.Sub(s.lastPerCore) < s.PerCoreEvery {
		return metrics, nil
	}
	s.lastPerCore = now

	percentages, err := cpu.Percent(0, true)
	if err != nil {
		log.Println(err)
		return metrics, nil
	}

	for i, percentage := range percentages {
		m := newMetric("cpu_usage_percent", percentage, pb.MetricType_METRIC_TYPE_CPU)
		m.Labels = map[string]string{"core": strconv.FormatInt(int64(i), 10)}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

func (s *DiskIOSource) Collect() ([]*pb.Metric, error) {
	counters, err := disk.IOCounters()
	if err != nil {
		return nil, err
	}

	var totalWrite uint64
	for _, counter := range counters {
		totalWrite += counter.WriteBytes
	}

	now := time.Now()
	if s.prevTime.IsZero() {
		s.prevWrite, s.prevTime = totalWrite, now
		return nil, nil
	}

	if totalWrite < s.prevWrite {
		s.prevWrite, s.prevTime = totalWrite, now
		return nil, nil
	}

	dt := now.Sub(s.prevTime).Seconds()
	rate := float64(totalWrite-s.prevWrite) / dt
	s.prevWrite, s.prevTime = totalWrite, now

	return []*pb.Metric{
		newMetric("disk_write_bytes_per_sec", rate, pb.MetricType_METRIC_TYPE_DISK),
	}, nil
}

type RAMSource struct {
}

func (s *RAMSource) Collect() ([]*pb.Metric, error) {
	vMemStat, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	return []*pb.Metric{
		newMetric("used_percent", vMemStat.UsedPercent, pb.MetricType_METRIC_TYPE_RAM),
		newMetric("total_bytes", float64(vMemStat.Total), pb.MetricType_METRIC_TYPE_RAM),
	}, nil
}

type DiskSource struct {
}

func (s *DiskSource) Collect() ([]*pb.Metric, error) {
	var metrics []*pb.Metric
	partitions, err := disk.Partitions(false)
	if err != nil {
		return nil, err
	}

	for _, partition := range partitions {
		usage, err := disk.Usage(partition.Mountpoint)
		if err != nil {
			log.Println(err)
		}

		m := newMetric("disk_usage_percent", usage.UsedPercent, pb.MetricType_METRIC_TYPE_DISK)
		m.Labels = map[string]string{"mountpoint": partition.Mountpoint, "device": partition.Device}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

func newMetric(
	metricName string,
	value float64,
	mType pb.MetricType,
) *pb.Metric {
	return &pb.Metric{
		Host:      host,
		Timestamp: timestamppb.Now(),
		Type:      mType,
		Value:     value,
		Name:      metricName,
	}
}
