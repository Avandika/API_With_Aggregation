package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadExecution(t *testing.T) {
	// Create a test server using our handler
	server := httptest.NewServer(http.HandlerFunc(handlePostSubmissions))
	defer server.Close()

	// Optimize the HTTP client to reuse ports cleanly under high load
	customTransport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Transport: customTransport,
	}

	var successCount int64
	var duplicateCount int64
	var errorCount int64

	totalRequests := 100000
	uniqueLimit := 80000

	var wg sync.WaitGroup
	// Throttle down concurrent workers slightly so Windows handles the port pool safely
	ch := make(chan int, 20)

	startTime := time.Now()

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		ch <- 1

		go func(index int) {
			defer wg.Done()
			defer func() { <-ch }()

			// Math logic to inject exactly 20,000 duplicate keys
			keyIndex := index
			if index >= uniqueLimit {
				keyIndex = index - uniqueLimit
			}

			jsonPayload := fmt.Sprintf(`{
				"form_id": "contact-form",
				"idempotency_key": "key-%d",
				"email": "test-%d@example.com",
				"data": {"message": "load test"}
			}`, keyIndex, keyIndex)

			resp, err := client.Post(server.URL, "application/json", bytes.NewBufferString(jsonPayload))
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusCreated {
				atomic.AddInt64(&successCount, 1)
			} else if resp.StatusCode == http.StatusOK {
				atomic.AddInt64(&duplicateCount, 1)
			} else {
				atomic.AddInt64(&errorCount, 1)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// Explicitly print our clear table metrics
	fmt.Println("\n--- LOAD TEST METRICS REPORT ---")
	fmt.Printf("Total Requests Sent: %d\n", totalRequests)
	fmt.Printf("Submissions Stored (Unique): %d\n", successCount)
	fmt.Printf("Duplicates Correctly Handled: %d\n", duplicateCount)
	fmt.Printf("Errors Occurred: %d\n", errorCount)
	fmt.Printf("Time Taken: %v\n", duration)
	fmt.Printf("Throughput Rate: %.2f requests/sec\n", float64(totalRequests)/duration.Seconds())
	fmt.Println("--------------------------------")
}
