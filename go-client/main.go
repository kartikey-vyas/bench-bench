package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Config struct {
	BaseURL           string  `json:"base_url"`
	DurationSeconds   float64 `json:"duration_seconds"`
	WarmupSeconds     float64 `json:"warmup_seconds"`
	Concurrency       int     `json:"concurrency"`
	ChunksPerResponse int     `json:"chunks_per_response"`
	ChunkBytes        int     `json:"chunk_bytes"`
	TTFCMS            int     `json:"ttfc_ms"`
	EventsPerSecond   int     `json:"events_per_second"`
	OutputDir         string  `json:"output_dir"`
}

type Measurement struct {
	OK           bool
	LatencyMS    float64
	FirstChunkMS float64
	Chunks       int
	Bytes        int
	MaxGapMS     float64
	StreamMS     float64
}

type Summary struct {
	DurationMS             float64 `json:"duration_ms"`
	SuccessfulRequests     int     `json:"successful_requests"`
	IncompleteRequests     int     `json:"incomplete_requests"`
	FailedRequests         int     `json:"failed_requests"`
	TotalChunks            int     `json:"total_chunks"`
	TotalBytes             int     `json:"total_bytes"`
	RequestsPerSecond      float64 `json:"requests_per_second"`
	ChunksPerSecond        float64 `json:"chunks_per_second"`
	MeanRequestLatencyMS   float64 `json:"mean_request_latency_ms"`
	P50RequestLatencyMS    float64 `json:"p50_request_latency_ms"`
	P95RequestLatencyMS    float64 `json:"p95_request_latency_ms"`
	P99RequestLatencyMS    float64 `json:"p99_request_latency_ms"`
	MeanTimeToFirstChunkMS float64 `json:"mean_time_to_first_chunk_ms"`
	P50TimeToFirstChunkMS  float64 `json:"p50_time_to_first_chunk_ms"`
	P95TimeToFirstChunkMS  float64 `json:"p95_time_to_first_chunk_ms"`
	P99TimeToFirstChunkMS  float64 `json:"p99_time_to_first_chunk_ms"`
	P50MaxGapMS            float64 `json:"p50_max_gap_ms"`
	P95MaxGapMS            float64 `json:"p95_max_gap_ms"`
	P99MaxGapMS            float64 `json:"p99_max_gap_ms"`
	MaxMaxGapMS            float64 `json:"max_max_gap_ms"`
	P50StreamStretch       float64 `json:"p50_stream_stretch"`
	P95StreamStretch       float64 `json:"p95_stream_stretch"`
	P99StreamStretch       float64 `json:"p99_stream_stretch"`
	IdealEventsPerSecond   float64 `json:"ideal_events_per_second"`
	Efficiency             float64 `json:"efficiency"`
}

type Result struct {
	Language       string  `json:"language"`
	Implementation string  `json:"implementation"`
	StartedAt      string  `json:"started_at"`
	Config         Config  `json:"config"`
	Summary        Summary `json:"summary"`
}

type sseDecoder struct {
	buffer string
}

func newSSEDecoder() *sseDecoder {
	return &sseDecoder{}
}

func (decoder *sseDecoder) feed(text string) []string {
	decoder.buffer += normalizeNewlines(text)
	events := []string{}

	for {
		index := strings.Index(decoder.buffer, "\n\n")
		if index < 0 {
			break
		}

		rawEvent := decoder.buffer[:index]
		decoder.buffer = decoder.buffer[index+2:]
		dataLines := []string{}

		for _, line := range strings.Split(rawEvent, "\n") {
			if line == "" || strings.HasPrefix(line, ":") {
				continue
			}
			if strings.HasPrefix(line, "data:") {
				value := strings.TrimPrefix(line, "data:")
				value = strings.TrimPrefix(value, " ")
				dataLines = append(dataLines, value)
			}
		}

		if len(dataLines) > 0 {
			events = append(events, strings.Join(dataLines, "\n"))
		}
	}

	return events
}

func normalizeNewlines(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	return strings.ReplaceAll(text, "\r", "\n")
}

func loadConfig(path string) (Config, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(content, &config); err != nil {
		return Config{}, err
	}
	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (config Config) validate() error {
	if config.DurationSeconds <= 0 {
		return fmt.Errorf("duration_seconds must be > 0")
	}
	if config.WarmupSeconds < 0 {
		return fmt.Errorf("warmup_seconds must be >= 0")
	}
	if config.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0")
	}
	if config.ChunksPerResponse <= 0 {
		return fmt.Errorf("chunks_per_response must be > 0")
	}
	if config.ChunkBytes <= 0 {
		return fmt.Errorf("chunk_bytes must be > 0")
	}
	if config.TTFCMS < 0 {
		return fmt.Errorf("ttfc_ms must be >= 0")
	}
	if config.EventsPerSecond < 0 {
		return fmt.Errorf("events_per_second must be >= 0")
	}
	return nil
}

func (config Config) endpoint() string {
	return strings.TrimRight(config.BaseURL, "/") + "/v1/chat/completions"
}

func (config Config) requestPayload(workerIndex int, sequence int, language string) map[string]any {
	return map[string]any{
		"model":             "synthetic",
		"messages":          []map[string]string{{"role": "user", "content": "benchmark"}},
		"stream":            true,
		"chunks":            config.ChunksPerResponse,
		"chunk_bytes":       config.ChunkBytes,
		"ttfc_ms":           config.TTFCMS,
		"events_per_second": config.EventsPerSecond,
		"request_id":        fmt.Sprintf("%s-%d-%d", language, workerIndex, sequence),
	}
}

func runOneRequest(client *http.Client, config Config, workerIndex int, sequence int) Measurement {
	started := time.Now()
	var firstEventAt, previousEventAt, lastEventAt time.Time
	maxGapMS := 0.0
	chunks := 0
	contentBytes := 0
	sawDone := false

	observe := func() {
		now := time.Now()
		if firstEventAt.IsZero() {
			firstEventAt = now
		}
		if !previousEventAt.IsZero() {
			gap := float64(now.Sub(previousEventAt).Microseconds()) / 1000.0
			if gap > maxGapMS {
				maxGapMS = gap
			}
		}
		previousEventAt = now
		lastEventAt = now
	}

	build := func(ok bool) Measurement {
		firstChunkMS := 0.0
		if !firstEventAt.IsZero() {
			firstChunkMS = float64(firstEventAt.Sub(started).Microseconds()) / 1000.0
		}
		streamMS := 0.0
		if !firstEventAt.IsZero() && !lastEventAt.IsZero() {
			streamMS = float64(lastEventAt.Sub(firstEventAt).Microseconds()) / 1000.0
		}
		return Measurement{
			OK:           ok,
			LatencyMS:    float64(time.Since(started).Microseconds()) / 1000.0,
			FirstChunkMS: firstChunkMS,
			Chunks:       chunks,
			Bytes:        contentBytes,
			MaxGapMS:     maxGapMS,
			StreamMS:     streamMS,
		}
	}

	body, err := json.Marshal(config.requestPayload(workerIndex, sequence, "go"))
	if err != nil {
		return build(false)
	}

	request, err := http.NewRequest(http.MethodPost, config.endpoint(), bytes.NewReader(body))
	if err != nil {
		return build(false)
	}
	request.Header.Set("content-type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		return build(false)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		return build(false)
	}

	reader := bufio.NewReader(response.Body)
	decoder := newSSEDecoder()

	for {
		piece, readErr := reader.ReadString('\n')
		if len(piece) > 0 {
			for _, event := range decoder.feed(piece) {
				observe()
				if event == "[DONE]" {
					sawDone = true
					continue
				}

				var payload struct {
					Choices []struct {
						Delta struct {
							Content string `json:"content"`
						} `json:"delta"`
					} `json:"choices"`
				}
				if err := json.Unmarshal([]byte(event), &payload); err != nil {
					return build(false)
				}
				if len(payload.Choices) == 0 {
					return build(false)
				}
				content := payload.Choices[0].Delta.Content
				if content != "" {
					chunks++
					contentBytes += len(content)
				}
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return build(false)
		}
	}

	return build(sawDone)
}

func runFor(config Config, seconds float64, client *http.Client) ([]Measurement, float64) {
	if seconds <= 0 {
		return nil, 0
	}
	started := time.Now()
	deadline := started.Add(time.Duration(seconds * float64(time.Second)))

	var mu sync.Mutex
	measurements := []Measurement{}
	var wg sync.WaitGroup

	for worker := 0; worker < config.Concurrency; worker++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			for sequence := 0; time.Now().Before(deadline); sequence++ {
				measurement := runOneRequest(client, config, workerIndex, sequence)
				mu.Lock()
				measurements = append(measurements, measurement)
				mu.Unlock()
			}
		}(worker)
	}

	wg.Wait()
	return measurements, float64(time.Since(started).Microseconds()) / 1000.0
}

func aggregateSummary(measurements []Measurement, durationMS float64, config Config) Summary {
	expected := config.ChunksPerResponse
	latencies := []float64{}
	firstChunks := []float64{}
	maxGaps := []float64{}
	stretches := []float64{}
	successful := 0
	incomplete := 0
	failed := 0
	totalChunks := 0
	totalBytes := 0

	idealStreamMS := 0.0
	if config.EventsPerSecond > 0 && expected > 1 {
		idealStreamMS = float64(expected-1) / float64(config.EventsPerSecond) * 1000.0
	}

	for _, measurement := range measurements {
		if !measurement.OK {
			failed++
			continue
		}
		if measurement.Chunks != expected {
			incomplete++
			continue
		}
		successful++
		latencies = append(latencies, measurement.LatencyMS)
		firstChunks = append(firstChunks, measurement.FirstChunkMS)
		maxGaps = append(maxGaps, measurement.MaxGapMS)
		if idealStreamMS > 0 {
			stretches = append(stretches, measurement.StreamMS/idealStreamMS)
		}
		totalChunks += measurement.Chunks
		totalBytes += measurement.Bytes
	}

	durationSeconds := durationMS / 1000.0
	chunksPerSecond := 0.0
	requestsPerSecond := 0.0
	if durationSeconds > 0 {
		chunksPerSecond = float64(totalChunks) / durationSeconds
		requestsPerSecond = float64(successful) / durationSeconds
	}
	idealEventsPerSecond := float64(config.EventsPerSecond * config.Concurrency)
	efficiency := 0.0
	if idealEventsPerSecond > 0 {
		efficiency = chunksPerSecond / idealEventsPerSecond
	}
	maxMaxGap := 0.0
	for _, gap := range maxGaps {
		if gap > maxMaxGap {
			maxMaxGap = gap
		}
	}

	return Summary{
		DurationMS:             durationMS,
		SuccessfulRequests:     successful,
		IncompleteRequests:     incomplete,
		FailedRequests:         failed,
		TotalChunks:            totalChunks,
		TotalBytes:             totalBytes,
		RequestsPerSecond:      requestsPerSecond,
		ChunksPerSecond:        chunksPerSecond,
		MeanRequestLatencyMS:   mean(latencies),
		P50RequestLatencyMS:    percentile(latencies, 0.50),
		P95RequestLatencyMS:    percentile(latencies, 0.95),
		P99RequestLatencyMS:    percentile(latencies, 0.99),
		MeanTimeToFirstChunkMS: mean(firstChunks),
		P50TimeToFirstChunkMS:  percentile(firstChunks, 0.50),
		P95TimeToFirstChunkMS:  percentile(firstChunks, 0.95),
		P99TimeToFirstChunkMS:  percentile(firstChunks, 0.99),
		P50MaxGapMS:            percentile(maxGaps, 0.50),
		P95MaxGapMS:            percentile(maxGaps, 0.95),
		P99MaxGapMS:            percentile(maxGaps, 0.99),
		MaxMaxGapMS:            maxMaxGap,
		P50StreamStretch:       percentile(stretches, 0.50),
		P95StreamStretch:       percentile(stretches, 0.95),
		P99StreamStretch:       percentile(stretches, 0.99),
		IdealEventsPerSecond:   idealEventsPerSecond,
		Efficiency:             efficiency,
	}
}

func percentile(values []float64, rank float64) float64 {
	if len(values) == 0 {
		return 0
	}

	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	index := int(math.Ceil(rank*float64(len(ordered)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func runBenchmark(config Config, outputDir string) (Result, error) {
	transport := &http.Transport{
		MaxIdleConns:        config.Concurrency,
		MaxIdleConnsPerHost: config.Concurrency,
	}
	client := &http.Client{Transport: transport}
	startedAt := time.Now().UTC()

	if config.WarmupSeconds > 0 {
		_, _ = runFor(config, config.WarmupSeconds, client)
	}

	measurements, durationMS := runFor(config, config.DurationSeconds, client)

	result := Result{
		Language:       "go",
		Implementation: "net-http-goroutines",
		StartedAt:      startedAt.Format(time.RFC3339Nano),
		Config:         config,
		Summary:        aggregateSummary(measurements, durationMS, config),
	}

	destination := outputDir
	if destination == "" {
		destination = filepath.Join(config.OutputDir, "go")
	}
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return result, err
	}

	content, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return result, err
	}
	content = append(content, '\n')
	if err := os.WriteFile(filepath.Join(destination, "summary.json"), content, 0o644); err != nil {
		return result, err
	}

	return result, nil
}

func main() {
	configPath := flag.String("config", "config/workload.smoke.json", "Path to workload JSON.")
	outputDir := flag.String("output-dir", "", "Directory for summary.json.")
	flag.Parse()

	config, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	result, err := runBenchmark(config, *outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run benchmark: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf(
		"go requests/s=%.2f chunks/s=%.2f efficiency=%.3f failures=%d incomplete=%d\n",
		result.Summary.RequestsPerSecond,
		result.Summary.ChunksPerSecond,
		result.Summary.Efficiency,
		result.Summary.FailedRequests,
		result.Summary.IncompleteRequests,
	)
}
