package main

import pb "github.com/honnek/vigil/proto"

type Rule struct {
	Name      string
	Metric    string
	Threshold float64
}

var rules = []Rule{
	{Name: "high_cpu", Metric: "cpu_usage_percent", Threshold: 5},
	{Name: "high_ram", Metric: "used_percent", Threshold: 5},
	{Name: "high_disk", Metric: "disk_usage_percent", Threshold: 5},
}

// Evaluate возвращает правила, сработавшие на данной метрике.
func Evaluate(m *pb.Metric) []Rule {
	var fired []Rule
	for _, r := range rules {
		if m.GetName() == r.Metric && m.GetValue() > r.Threshold {
			fired = append(fired, r)
		}
	}
	return fired
}
