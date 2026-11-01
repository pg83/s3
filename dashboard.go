package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed cluster.html
var clusterHTML string

//go:embed cluster.css
var clusterCSS string

//go:embed cluster.js
var clusterJS string

type DashboardConfig struct {
	Prometheus string `json:"prometheus"`
	EtcdJob    string `json:"etcd_job"`
}

type Dashboard struct {
	cfg     *Config
	client  *http.Client
	mu      sync.RWMutex
	data    map[string]any
	failure string
}

type PromSeries struct {
	Metric map[string]string   `json:"metric"`
	Value  []json.RawMessage   `json:"value"`
	Values [][]json.RawMessage `json:"values"`
}

type PromResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		Result []PromSeries `json:"result"`
	} `json:"data"`
}

type DashboardMetrics map[string][]PromSeries

func newDashboard(cfg *Config) *Dashboard {
	d := &Dashboard{cfg: cfg, client: &http.Client{Timeout: 12 * time.Second}}

	if cfg.Dashboard != nil && cfg.Dashboard.Prometheus != "" {
		if cfg.Dashboard.EtcdJob == "" {
			cfg.Dashboard.EtcdJob = "etcd_2"
		}

		go d.loop()
	}

	return d
}

func (d *Dashboard) loop() {
	ticker := time.NewTicker(30 * time.Second)

	defer ticker.Stop()

	for {
		err := try(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)

			defer cancel()

			now := time.Now().UTC()
			data := d.snapshot(d.metrics(ctx, now), now)
			d.mu.Lock()
			d.data = data
			d.failure = ""
			d.mu.Unlock()
		})

		err.catch(func(exc *Exception) {
			slog.Error("cluster metrics", "err", exc.error())
			d.mu.Lock()
			d.failure = "Prometheus is unavailable"
			d.mu.Unlock()
		})

		<-ticker.C
	}
}

func (d *Dashboard) query(ctx context.Context, now time.Time, name, expression string) []PromSeries {
	endpoint := "query"
	args := url.Values{"query": {expression}, "time": {strconv.FormatInt(now.Unix(), 10)}}

	if strings.HasSuffix(name, "_history") {
		endpoint = "query_range"
		args = url.Values{"query": {expression}, "start": {strconv.FormatInt(now.Unix()-86400, 10)}, "end": {strconv.FormatInt(now.Unix(), 10)}, "step": {"300"}}
	}

	req := throw2(http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(d.cfg.Dashboard.Prometheus, "/")+"/api/v1/"+endpoint+"?"+args.Encode(), nil))
	resp := throw2(d.client.Do(req))

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		throwFmt("prometheus %s: HTTP %d", name, resp.StatusCode)
	}

	var result PromResponse

	throw(json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&result))

	if result.Status != "success" {
		throwFmt("prometheus %s: %s", name, result.Error)
	}

	return result.Data.Result
}

func (d *Dashboard) metrics(ctx context.Context, now time.Time) DashboardMetrics {
	etcd := "{job=" + strconv.Quote(d.cfg.Dashboard.EtcdJob) + "}"
	queries := map[string]string{
		"gauges":                 `{__name__=~"s3_cell_(capacity_bytes|head_bytes|hdd_pointer_bytes|lru_bytes|flush_queue_blocks|read_queue_requests|write_queue_requests)|s3_volume_(info|capacity_bytes|used_bytes|free_bytes|filesystem_success|collect_timestamp_seconds)|smart_device_(info|scrape_success|health_passed|temperature_celsius)|smart_collect_timestamp_seconds|smart_ata_attribute_raw_value|etcd_server_(has_leader|is_leader|quota_backend_bytes)|etcd_mvcc_db_total_size_in_bytes"}`,
		"up":                     `up{job=~"s3_cell_[0-9]+|etcd_.*|node_exporter"}`,
		"read_rps":               `sum by (host,job) (rate(s3_cell_reads_total[5m]))`,
		"write_rps":              `sum by (host,job) (rate(s3_cell_writes_total[5m]))`,
		"read_bps":               `sum by (host,job) (rate(s3_cell_read_bytes_total[5m]))`,
		"write_bps":              `sum by (host,job) (rate(s3_cell_write_bytes_total[5m]))`,
		"growth":                 `deriv(s3_cell_head_bytes[1h])`,
		"errors":                 `sum by (host,job) (increase(s3_cell_errors_total[24h]))`,
		"rejected_reads":         `sum by (host,job) (increase(s3_cell_reads_total{result!="ok"}[24h]))`,
		"rejected_writes":        `sum by (host,job) (increase(s3_cell_writes_total{result!="ok"}[24h]))`,
		"capacity_history":       `s3_cell_head_bytes`,
		"rps_history":            `sum by (host,job) (rate(s3_cell_reads_total[5m])) + sum by (host,job) (rate(s3_cell_writes_total[5m]))`,
		"load_history":           `s3_cell_lru_bytes`,
		"store_history":          `s3_cell_flush_queue_blocks * 2097152`,
		"load_activity_history":  `sum by (host,job) (rate(s3_cell_blocks_total{source="lru"}[5m]))`,
		"store_activity_history": `sum by (host,job) (rate(s3_cell_writes_total[5m]))`,
		"load_growth":            `deriv(s3_cell_lru_bytes[1h])`,
		"store_growth":           `deriv(s3_cell_flush_queue_blocks[1h]) * 2097152`,
		"etcd_db_history":        "etcd_mvcc_db_total_size_in_bytes" + etcd,
		"etcd_rps_history":       "sum by (host,job) (rate(grpc_server_handled_total{job=" + strconv.Quote(d.cfg.Dashboard.EtcdJob) + `,grpc_type="unary"}[5m]))`,
		"etcd_growth":            "deriv(etcd_mvcc_db_total_size_in_bytes" + etcd + "[1h])",
		"etcd_errors":            "increase(etcd_server_proposals_failed_total" + etcd + "[24h])",
		"etcd_fsync":             "histogram_quantile(0.99, sum by (host,job,le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket" + etcd + "[5m])))",
		"smart_delta":            `delta(smart_ata_attribute_raw_value{attribute_id=~"5|197|198",disk_type="hdd"}[24h])`,
		"volume_iops":            `rate(s3_volume_read_operations_total[5m]) + rate(s3_volume_write_operations_total[5m])`,
		"volume_read_bps":        `rate(s3_volume_read_bytes_total[5m])`,
		"volume_write_bps":       `rate(s3_volume_write_bytes_total[5m])`,
	}
	result := DashboardMetrics{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var failure *Exception
	limit := make(chan struct{}, 4)

	for name, expression := range queries {
		wg.Add(1)

		go func() {
			defer wg.Done()

			limit <- struct{}{}

			defer func() { <-limit }()

			err := try(func() {
				value := d.query(ctx, now, name, expression)
				mu.Lock()
				result[name] = value
				mu.Unlock()
			})

			if err != nil {
				mu.Lock()
				failure = err
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if failure != nil {
		failure.throw()
	}

	return result
}

func promNumber(raw json.RawMessage) *float64 {
	var text string

	throw(json.Unmarshal(raw, &text))
	n := throw2(strconv.ParseFloat(text, 64))

	if math.IsNaN(n) || math.IsInf(n, 0) {
		return nil
	}

	return &n
}

func (m DashboardMetrics) find(group string, labels map[string]string) *PromSeries {
	for _, sample := range m[group] {
		matched := true

		for key, value := range labels {
			if sample.Metric[key] != value {
				matched = false

				break
			}
		}

		if matched {
			return &sample
		}
	}

	return nil
}

func (m DashboardMetrics) value(group string, labels map[string]string) *float64 {
	sample := m.find(group, labels)

	if sample == nil || len(sample.Value) != 2 {
		return nil
	}

	return promNumber(sample.Value[1])
}

func (m DashboardMetrics) gauge(name string, labels map[string]string) *float64 {
	return m.value("gauges", withLabel(labels, "__name__", name))
}

func withLabel(labels map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(labels)+1)

	for k, v := range labels {
		result[k] = v
	}

	result[key] = value

	return result
}

func (m DashboardMetrics) history(group string, labels map[string]string) [][2]float64 {
	result := [][2]float64{}
	sample := m.find(group, labels)

	if sample == nil {
		return result
	}

	for _, value := range sample.Values {
		if len(value) != 2 {
			continue
		}

		y := promNumber(value[1])

		if y == nil {
			continue
		}

		var t float64

		throw(json.Unmarshal(value[0], &t))
		result = append(result, [2]float64{t, *y})
	}

	return result
}

func freshTimestamp(stamp *float64, now time.Time, seconds float64) bool {
	return stamp != nil && now.Unix()-int64(*stamp) < int64(seconds)
}

func isOne(v *float64) bool {
	return v != nil && *v == 1
}

func (m DashboardMetrics) smart(host, device string, now time.Time) map[string]any {
	labels := map[string]string{"host": host, "device": device}
	smart := map[string]any{"device": device, "model": device, "serial_suffix": "", "health": nil, "scrape": nil, "temperature": nil}
	info := m.find("gauges", withLabel(labels, "__name__", "smart_device_info"))

	if info != nil {
		smart["model"] = info.Metric["model"]
		serial := info.Metric["serial"]

		if len(serial) > 6 {
			serial = serial[len(serial)-6:]
		}

		smart["serial_suffix"] = serial
	}

	fresh := freshTimestamp(m.gauge("smart_collect_timestamp_seconds", map[string]string{"host": host}), now, 3600)

	for field, name := range map[string]string{"health": "smart_device_health_passed", "temperature": "smart_device_temperature_celsius", "scrape": "smart_device_scrape_success"} {
		if fresh {
			smart[field] = m.gauge(name, labels)
		}
	}

	for field, attribute := range map[string]string{"reallocated": "5", "pending": "197", "uncorrectable": "198"} {
		smart[field] = nil
		smart[field+"_delta"] = nil

		if fresh {
			smart[field] = m.gauge("smart_ata_attribute_raw_value", withLabel(labels, "attribute_id", attribute))
			smart[field+"_delta"] = m.value("smart_delta", withLabel(labels, "attribute_id", attribute))
		}
	}

	return smart
}

func (m DashboardMetrics) volume(host, role string, index int, now time.Time) map[string]any {
	labels := map[string]string{"host": host, "role": role, "cell_index": strconv.Itoa(index)}
	stamp := m.gauge("s3_volume_collect_timestamp_seconds", map[string]string{"host": host})
	fresh := freshTimestamp(stamp, now, 100)
	part := map[string]any{
		"id": host + "-" + role + "-" + strconv.Itoa(index), "host": host, "role": role, "index": index,
		"device": "—", "mount": "", "up": 0, "capacity": nil, "used": nil, "free": nil, "captured_at": stamp,
		"smart": m.smart(host, "", now), "iops": nil, "read_bps": nil, "write_bps": nil, "io_sample_seconds": 300,
		"label": "S3_" + strings.ToUpper(role) + "_" + strconv.Itoa(index),
	}

	if role == "etcd" {
		part["index"] = nil
		part["label"] = "S3_ETCD"
	}

	info := m.find("gauges", withLabel(labels, "__name__", "s3_volume_info"))

	if info != nil {
		part["device"] = info.Metric["device"]
		part["mount"] = info.Metric["mount"]
		part["smart"] = m.smart(host, info.Metric["disk"], now)
	}

	if fresh {
		part["iops"] = m.value("volume_iops", labels)
		part["read_bps"] = m.value("volume_read_bps", labels)
		part["write_bps"] = m.value("volume_write_bps", labels)

		if isOne(m.gauge("s3_volume_filesystem_success", labels)) {
			part["up"] = 1

			for _, field := range []string{"capacity", "used", "free"} {
				part[field] = m.gauge("s3_volume_"+field+"_bytes", labels)
			}
		}
	}

	return part
}

func (d *Dashboard) snapshot(m DashboardMetrics, now time.Time) map[string]any {
	byHost := map[string][]CellSpec{}

	for _, spec := range d.cfg.Cells {
		byHost[spec.Host] = append(byHost[spec.Host], spec)
	}

	names := []string{}

	for host := range byHost {
		names = append(names, host)
	}

	sort.Strings(names)
	hosts := []map[string]any{}
	cells := []map[string]any{}
	parts := []map[string]any{}

	for _, host := range names {
		specs := byHost[host]
		sort.Slice(specs, func(i, j int) bool { return specs[i].Id < specs[j].Id })
		hosts = append(hosts, map[string]any{"name": host})
		etcd := m.volume(host, "etcd", -1, now)
		labels := map[string]string{"host": host, "job": d.cfg.Dashboard.EtcdJob}

		for field, name := range map[string]string{"has_leader": "etcd_server_has_leader", "is_leader": "etcd_server_is_leader", "db_size": "etcd_mvcc_db_total_size_in_bytes", "quota": "etcd_server_quota_backend_bytes"} {
			etcd[field] = m.gauge(name, labels)
		}

		etcd["capacity_history"] = m.history("etcd_db_history", labels)
		rpcHistory := m.history("etcd_rps_history", labels)
		etcd["rps_history"] = rpcHistory
		etcd["activity"] = lastValue(rpcHistory)
		etcd["growth"] = m.value("etcd_growth", labels)
		etcd["errors"] = m.value("etcd_errors", labels)
		etcd["fsync_p99"] = m.value("etcd_fsync", labels)
		etcd["capacity_label"] = "Database size"
		etcd["activity_label"] = "RPC/s"
		etcd["etcd_job"] = d.cfg.Dashboard.EtcdJob

		if !isOne(m.value("up", labels)) {
			etcd["up"] = 0
		}

		parts = append(parts, etcd)

		for index, spec := range specs {
			labels = map[string]string{"host": host, "job": fmt.Sprintf("s3_cell_%d", index)}
			volume := m.volume(host, "hdd", index, now)
			cell := map[string]any{"id": spec.Id, "host": host, "index": index, "smart": volume["smart"], "device": volume["device"]}
			cell["stale"] = !freshTimestamp(m.gauge("s3_volume_collect_timestamp_seconds", map[string]string{"host": host}), now, 100)

			for field, metric := range map[string]string{"capacity": "capacity_bytes", "used": "head_bytes", "on_hdd": "hdd_pointer_bytes", "cache": "lru_bytes", "flush_queue": "flush_queue_blocks", "read_queue": "read_queue_requests", "write_queue": "write_queue_requests"} {
				cell[field] = m.gauge("s3_cell_"+metric, labels)
			}

			for _, field := range []string{"up", "read_rps", "write_rps", "read_bps", "write_bps", "growth"} {
				cell[field] = m.value(field, labels)
			}

			var failures *float64

			if isOne(m.value("up", labels)) {
				total := float64(0)

				for _, field := range []string{"errors", "rejected_reads", "rejected_writes"} {
					if count := m.value(field, labels); count != nil {
						total += *count
					}
				}

				failures = &total
			}

			cell["errors"] = failures
			cell["capacity_history"] = m.history("capacity_history", labels)
			cell["rps_history"] = m.history("rps_history", labels)
			cells = append(cells, cell)

			for _, role := range []string{"store", "load"} {
				part := m.volume(host, role, index, now)
				part["cell_id"] = spec.Id
				part["capacity_history"] = m.history(role+"_history", labels)
				history := m.history(role+"_activity_history", labels)
				part["rps_history"] = history
				part["activity"] = lastValue(history)
				part["growth"] = m.value(role+"_growth", labels)
				part["errors"] = nil
				part["capacity_label"] = "Queued payload"
				part["activity_label"] = "Appends/s"

				if role == "load" {
					part["capacity_label"] = "Cached payload"
					part["activity_label"] = "Cache hits/s"
				}

				if !isOne(m.value("up", labels)) {
					part["up"] = 0
				}

				parts = append(parts, part)
			}
		}
	}

	return map[string]any{"timestamp": now.Unix(), "captured_at": now.Format(time.RFC3339), "hosts": hosts, "cells": cells, "partitions": parts, "buckets": d.cfg.Buckets}
}

func lastValue(history [][2]float64) *float64 {
	if len(history) == 0 {
		return nil
	}

	v := history[len(history)-1][1]

	return &v
}

func (d *Dashboard) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	d.mu.RLock()
	data := map[string]any{}

	for k, v := range d.data {
		data[k] = v
	}

	data["error"] = d.failure
	ready := d.data != nil
	d.mu.RUnlock()

	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
		data["error"] = "Waiting for Prometheus"

		if d.cfg.Dashboard == nil || d.cfg.Dashboard.Prometheus == "" {
			data["error"] = "Cluster metrics are not configured"
		}
	}

	_ = json.NewEncoder(w).Encode(data)
}
