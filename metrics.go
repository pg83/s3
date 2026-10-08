package main

import (
	"bytes"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"
)

var (
	latencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	metricEvents   = make(chan metricEvent, 4096)
	metricScrapes  = make(chan chan []byte)
)

type metricEvent struct {
	name   string
	labels string
	value  float64
	timing bool
}

type series struct {
	name   string
	labels string
}

type histogram struct {
	buckets []uint64
	sum     float64
	count   uint64
}

type gauge struct {
	name   string
	labels string
	value  float64
}

func metricAdd(name, labels string, n float64) {
	metricEvents <- metricEvent{name: name, labels: labels, value: n}
}

func metricTime(name, labels string, d time.Duration) {
	metricEvents <- metricEvent{name: name, labels: labels, value: d.Seconds(), timing: true}
}

func collectMetrics() {
	counters := map[series]float64{}
	timings := map[series]*histogram{}

	for {
		select {
		case e := <-metricEvents:
			s := series{e.name, e.labels}

			if !e.timing {
				counters[s] += e.value

				continue
			}

			h := timings[s]

			if h == nil {
				h = &histogram{buckets: make([]uint64, len(latencyBuckets))}
				timings[s] = h
			}

			h.observe(e.value)
		case out := <-metricScrapes:
			out <- renderMetrics(counters, timings)
		}
	}
}

func (h *histogram) observe(v float64) {
	h.sum += v
	h.count++

	for i, le := range latencyBuckets {
		if v <= le {
			h.buckets[i]++

			return
		}
	}
}

func sortedSeries[V any](m map[series]V) []series {
	keys := make([]series, 0, len(m))

	for s := range m {
		keys = append(keys, s)
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}

		return keys[i].labels < keys[j].labels
	})

	return keys
}

func number(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func braces(labels ...string) string {
	out := ""

	for _, l := range labels {
		if l == "" {
			continue
		}

		if out != "" {
			out += ","
		}

		out += l
	}

	if out == "" {
		return ""
	}

	return "{" + out + "}"
}

func writeType(b *bytes.Buffer, typed map[string]bool, name, kind string) {
	if typed[name] {
		return
	}

	typed[name] = true
	b.WriteString("# TYPE " + name + " " + kind + "\n")
}

func renderMetrics(counters map[series]float64, timings map[series]*histogram) []byte {
	var b bytes.Buffer

	typed := map[string]bool{}

	for _, s := range sortedSeries(counters) {
		writeType(&b, typed, s.name, "counter")
		b.WriteString(s.name + braces(s.labels) + " " + number(counters[s]) + "\n")
	}

	for _, s := range sortedSeries(timings) {
		h := timings[s]
		total := uint64(0)

		writeType(&b, typed, s.name, "histogram")

		for i, le := range latencyBuckets {
			total += h.buckets[i]
			b.WriteString(s.name + "_bucket" + braces(s.labels, `le="`+number(le)+`"`) + " " + strconv.FormatUint(total, 10) + "\n")
		}

		b.WriteString(s.name + "_bucket" + braces(s.labels, `le="+Inf"`) + " " + strconv.FormatUint(h.count, 10) + "\n")
		b.WriteString(s.name + "_sum" + braces(s.labels) + " " + number(h.sum) + "\n")
		b.WriteString(s.name + "_count" + braces(s.labels) + " " + strconv.FormatUint(h.count, 10) + "\n")
	}

	return b.Bytes()
}

func renderGauges(b *bytes.Buffer, gauges []gauge) {
	sort.SliceStable(gauges, func(i, j int) bool { return gauges[i].name < gauges[j].name })

	typed := map[string]bool{}

	for _, g := range gauges {
		writeType(b, typed, g.name, "gauge")
		b.WriteString(g.name + braces(g.labels) + " " + number(g.value) + "\n")
	}
}

func serveMetrics(addr string, gauges func() []gauge) {
	if addr == "" {
		return
	}

	ln := sys.accepts(throw2(net.Listen("tcp", addr)))
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		out := make(chan []byte, 1)

		metricScrapes <- out

		body := bytes.NewBuffer(<-out)

		renderGauges(body, gauges())
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write(body.Bytes())
	})

	go func() { throw(http.Serve(ln, mux)) }()
}

func opName(op byte) string {
	switch op {
	case opAppend:
		return "append"
	case opRead:
		return "read"
	case opStatus:
		return "status"
	case opCancel:
		return "cancel"
	}

	return "unknown"
}

func codeName(code byte) string {
	switch code {
	case codeFull:
		return "full"
	case codeRange:
		return "range"
	case codeCancelled:
		return "cancelled"
	}

	return "io"
}
