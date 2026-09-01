package main

import "time"

type Alert struct {
	Host string `json:"host"`
	Rule string `json:"rule"`
}

type Log struct {
	TS      time.Time         `json:"ts"`
	Host    string            `json:"host"`
	Service string            `json:"service"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	TraceID string            `json:"trace_id,omitempty"`
	Fields  map[string]string `json:"fields,omitempty"`
}
