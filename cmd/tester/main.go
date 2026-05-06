package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	baseURL := getEnv("GONETPATH_BASE_URL", "http://localhost:3000")
	messagesPerCycle := 5

	flag.StringVar(&baseURL, "url", baseURL, "base URL of the gonetpath server")
	flag.IntVar(&messagesPerCycle, "messages", messagesPerCycle, "messages published per cycle")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	client := &http.Client{Timeout: 60 * time.Second}

	if err := runCycle(ctx, client, baseURL, messagesPerCycle); err != nil {
		log.Fatalf("cycle failed: %v", err)
	}
}

func runCycle(ctx context.Context, client *http.Client, baseURL string, count int) error {
	start := time.Now().UTC()
	log.Printf("cycle start: stamp=%s url=%s", start.Format(time.RFC3339Nano), baseURL)

	published := 0
	for i := 1; i <= count; i++ {
		msg := fmt.Sprintf("tester msg %d/%d @ %s", i, count, start.Format(time.RFC3339Nano))
		id, err := publish(ctx, client, baseURL, msg)
		if err != nil {
			log.Printf("publish %d/%d failed: %v", i, count, err)
			continue
		}
		published++
		log.Printf("published %d/%d id=%s", i, count, id)
	}

	messages, err := pull(ctx, client, baseURL)
	if err != nil {
		return fmt.Errorf("pull: %w", err)
	}
	log.Printf("cycle end: published=%d pulled=%d duration=%s", published, len(messages), time.Since(start))
	return nil
}

func publish(ctx context.Context, client *http.Client, baseURL, message string) (string, error) {
	body, err := json.Marshal(map[string]string{"message": message})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/publish", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}

	var out struct {
		MessageID string `json:"message_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.MessageID, nil
}

func pull(ctx context.Context, client *http.Client, baseURL string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/pull", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var out struct {
		Messages []string `json:"messages"`
		Count    int      `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
