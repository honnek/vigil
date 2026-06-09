package source

import (
	"log"
	"os"
	"strconv"

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
}

func (s *CPUSource) Collect() ([]*pb.Metric, error) {
	var metrics []*pb.Metric
	percentages, err := cpu.Percent(0, true)
	if err != nil {
		return nil, err
	}

	for i, percentage := range percentages {
		m := newMetric("cpu_usage_percent", percentage, pb.MetricType_METRIC_TYPE_CPU)
		m.Labels = map[string]string{"core": strconv.FormatInt(int64(i), 10)}
		metrics = append(metrics, m)
	}

	return metrics, nil
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
