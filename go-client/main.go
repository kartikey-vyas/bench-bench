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
	BaseURL           string `json:"base_url"`
	TotalRequests     int    `json:"total_requests"`
	Concurrency       int    `json:"concurrency"`
	ChunksPerResponse int    `json:"chunks_per_response"`
	ChunkBytes        int    `json:"chunk_bytes"`
	DelayUS           int    `json:"delay_us"`
	WarmupRequests    int    `json:"warmup_requests"`
	OutputDir         string `json:"output_dir"`
}

type Measurement struct {
	OK           bool
	LatencyMS    float64
	FirstChunkMS float64
	Chunks       int
	Bytes        int
}

type Summary struct {
	DurationMS              float64 `json:"duration_ms"`
	SuccessfulRequests      int     `json:"successful_requests"`
	FailedRequests          int     `json:"failed_requests"`
	TotalChunks             int     `json:"total_chunks"`
	TotalBytes              int     `json:"total_bytes"`
	RequestsPerSecond       float64 `json:"requests_per_second"`
	ChunksPerSecond         float64 `json:"chunks_per_second"`
	MeanRequestLatencyMS    float64 `json:"mean_request_latency_ms"`
	P50RequestLatencyMS     float64 `json:"p50_request_latency_ms"`
	P95RequestLatencyMS     float64 `json:"p95_request_latency_ms"`
	P99RequestLatencyMS     float64 `json:"p99_request_latency_ms"`
	MeanTimeToFirstChunkMS  float64 `json:"mean_time_to_first_chunk_ms"`
	P50TimeToFirstChunkMS   float64 `json:"p50_time_to_first_chunk_ms"`
	P95TimeToFirstChunkMS   float64 `json:"p95_time_to_first_chunk_ms"`
	P99TimeToFirstChunkMS   float64 `json:"p99_time_to_first_chunk_ms"`
	PerChunkOverheadMS      float64 `json:"per_chunk_overhead_ms"`
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
	if config.TotalRequests < 0 {
		return fmt.Errorf("total_requests must be >= 0")
	}
	if config.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0")
	}
	if config.ChunksPerResponse <= 0 {
		return fmt.Errorf("chunks_per_response must be > 0")
	}
	if config.ChunkBytes < 0 {
		return fmt.Errorf("chunk_bytes must be >= 0")
	}
	if config.DelayUS < 0 {
		return fmt.Errorf("delay_us must be >= 0")
	}
	if config.WarmupRequests < 0 {
		return fmt.Errorf("warmup_requests must be >= 0")
	}
	return nil
}

func (config Config) endpoint() string {
	return strings.TrimRight(config.BaseURL, "/") + "/v1/chat/completions"
}

func (config Config) requestPayload(index int, language string) map[string]any {
	return map[string]any{
		"model":       "synthetic",
		"messages":    []map[string]string{{"role": "user", "content": "benchmark"}},
		"stream":      true,
		"chunks":      config.ChunksPerResponse,
		"chunk_bytes": config.ChunkBytes,
		"delay_us":    config.DelayUS,
		"request_id":  fmt.Sprintf("%s-%d", language, index),
	}
}

func runOneRequest(client *http.Client, config Config, index int) Measurement {
	started := time.Now()
	body, err := json.Marshal(config.requestPayload(index, "go"))
	if err != nil {
		return failedMeasurement(started, 0, 0, 0)
	}

	request, err := http.NewRequest(http.MethodPost, config.endpoint(), bytes.NewReader(body))
	if err != nil {
		return failedMeasurement(started, 0, 0, 0)
	}
	request.Header.Set("content-type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		return failedMeasurement(started, 0, 0, 0)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		return failedMeasurement(started, 0, 0, 0)
	}

	reader := bufio.NewReader(response.Body)
	decoder := newSSEDecoder()
	firstChunkMS := 0.0
	chunks := 0
	contentBytes := 0
	sawDone := false

	for {
		piece, readErr := reader.ReadString('\n')
		if len(piece) > 0 {
			for _, event := range decoder.feed(piece) {
				if event == "[DONE]" {
					sawDone = true
					continue
				}

				if chunks == 0 {
					firstChunkMS = float64(time.Since(started).Microseconds()) / 1000.0
				}

				var payload struct {
					Choices []struct {
						Delta struct {
							Content string `json:"content"`
						} `json:"delta"`
					} `json:"choices"`
				}
				if err := json.Unmarshal([]byte(event), &payload); err != nil {
					return failedMeasurement(started, firstChunkMS, chunks, contentBytes)
				}
				if len(payload.Choices) == 0 {
					return failedMeasurement(started, firstChunkMS, chunks, contentBytes)
				}
				content := payload.Choices[0].Delta.Content
				chunks++
				contentBytes += len([]byte(content))
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return failedMeasurement(started, firstChunkMS, chunks, contentBytes)
		}
	}

	return Measurement{
		OK:           sawDone,
		LatencyMS:    float64(time.Since(started).Microseconds()) / 1000.0,
		FirstChunkMS: firstChunkMS,
		Chunks:       chunks,
		Bytes:        contentBytes,
	}
}

func failedMeasurement(started time.Time, firstChunkMS float64, chunks int, contentBytes int) Measurement {
	return Measurement{
		OK:           false,
		LatencyMS:    float64(time.Since(started).Microseconds()) / 1000.0,
		FirstChunkMS: firstChunkMS,
		Chunks:       chunks,
		Bytes:        contentBytes,
	}
}

func runMany(config Config, totalRequests int, client *http.Client) []Measurement {
	if totalRequests == 0 {
		return []Measurement{}
	}

	workerCount := config.Concurrency
	if workerCount > totalRequests {
		workerCount = totalRequests
	}

	jobs := make(chan int)
	results := make(chan Measurement, totalRequests)
	var wg sync.WaitGroup

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				results <- runOneRequest(client, config, index)
			}
		}()
	}

	for index := 0; index < totalRequests; index++ {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	close(results)

	measurements := make([]Measurement, 0, totalRequests)
	for measurement := range results {
		measurements = append(measurements, measurement)
	}
	return measurements
}

func aggregateSummary(measurements []Measurement, durationMS float64) Summary {
	successful := []Measurement{}
	failed := 0
	latencies := []float64{}
	firstChunks := []float64{}
	totalChunks := 0
	totalBytes := 0

	for _, measurement := range measurements {
		if measurement.OK {
			successful = append(successful, measurement)
			latencies = append(latencies, measurement.LatencyMS)
			firstChunks = append(firstChunks, measurement.FirstChunkMS)
			totalChunks += measurement.Chunks
			totalBytes += measurement.Bytes
		} else {
			failed++
		}
	}

	durationSeconds := durationMS / 1000.0
	summary := Summary{
		DurationMS:             durationMS,
		SuccessfulRequests:     len(successful),
		FailedRequests:         failed,
		TotalChunks:            totalChunks,
		TotalBytes:             totalBytes,
		MeanRequestLatencyMS:   mean(latencies),
		P50RequestLatencyMS:    percentile(latencies, 0.50),
		P95RequestLatencyMS:    percentile(latencies, 0.95),
		P99RequestLatencyMS:    percentile(latencies, 0.99),
		MeanTimeToFirstChunkMS: mean(firstChunks),
		P50TimeToFirstChunkMS:  percentile(firstChunks, 0.50),
		P95TimeToFirstChunkMS:  percentile(firstChunks, 0.95),
		P99TimeToFirstChunkMS:  percentile(firstChunks, 0.99),
	}
	if durationSeconds > 0 {
		summary.RequestsPerSecond = float64(len(successful)) / durationSeconds
		summary.ChunksPerSecond = float64(totalChunks) / durationSeconds
	}
	if totalChunks > 0 {
		summary.PerChunkOverheadMS = durationMS / float64(totalChunks)
	}

	return summary
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
	client := &http.Client{Timeout: 0}
	startedAt := time.Now().UTC()

	if config.WarmupRequests > 0 {
		_ = runMany(config, config.WarmupRequests, client)
	}

	measuredStart := time.Now()
	measurements := runMany(config, config.TotalRequests, client)
	durationMS := float64(time.Since(measuredStart).Microseconds()) / 1000.0

	result := Result{
		Language:       "go",
		Implementation: "net-http-goroutines",
		StartedAt:      startedAt.Format(time.RFC3339Nano),
		Config:         config,
		Summary:        aggregateSummary(measurements, durationMS),
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
		"go requests/s=%.2f chunks/s=%.2f failures=%d\n",
		result.Summary.RequestsPerSecond,
		result.Summary.ChunksPerSecond,
		result.Summary.FailedRequests,
	)
}
