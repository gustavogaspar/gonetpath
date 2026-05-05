package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	projectID      = "datadog-ese-sandbox"
	topicID        = "gustavo-ese-pubsub"
	subscriptionID = "netpath"
)

var tracer = otel.Tracer("gonetpath")

func publishMessage(ctx context.Context, client *pubsub.Client, message string) (string, error) {
	ctx, span := tracer.Start(ctx, "pubsub.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()
	span.SetAttributes(
		attribute.String("peer.service", "google-cloud-pubsub"),
		attribute.String("server.address", "pubsub.googleapis.com"),
		attribute.String("messaging.system", "gcp_pubsub"),
		attribute.String("messaging.destination.name", topicID),
		attribute.String("messaging.operation.name", "publish"),
		attribute.Int("messaging.message.body.size", len(message)),
	)

	publisher := client.Publisher(topicID)
	result := publisher.Publish(ctx, &pubsub.Message{
		Data: []byte(message),
	})

	msgID, err := result.Get(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	span.SetAttributes(attribute.String("messaging.message.id", msgID))
	return msgID, nil
}

func pullMessages(ctx context.Context, client *pubsub.Client, maxMessages int) ([]string, error) {
	ctx, span := tracer.Start(ctx, "pubsub.pull",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()
	span.SetAttributes(
		attribute.String("peer.service", "google-cloud-pubsub"),
		attribute.String("server.address", "pubsub.googleapis.com"),
		attribute.String("messaging.system", "gcp_pubsub"),
		attribute.String("messaging.destination.name", subscriptionID),
		attribute.String("messaging.operation.name", "receive"),
	)

	sub := client.Subscriber(subscriptionID)

	var (
		mu       sync.Mutex
		messages []string
	)

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		mu.Lock()
		messages = append(messages, string(msg.Data))
		if len(messages) >= maxMessages {
			cancel()
		}
		mu.Unlock()
	})

	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	span.SetAttributes(attribute.Int("messaging.batch.message_count", len(messages)))
	return messages, nil
}

func main() {
	ctx := context.Background()

	shutdown, err := initTracer(ctx)
	if err != nil {
		log.Fatalf("failed to initialize tracer: %v", err)
	}
	defer shutdown(ctx)

	client, err := pubsub.NewClient(ctx, projectID,
		option.WithGRPCDialOption(grpc.WithStatsHandler(otelgrpc.NewClientHandler())),
	)
	if err != nil {
		log.Fatalf("failed to create pubsub client: %v", err)
	}
	defer client.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			Message string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if body.Message == "" {
			http.Error(w, "message field is required", http.StatusBadRequest)
			return
		}

		msgID, err := publishMessage(r.Context(), client, body.Message)
		if err != nil {
			log.Printf("failed to publish message: %v", err)
			http.Error(w, "failed to publish message", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message_id": msgID})
	})

	mux.HandleFunc("/pull", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		messages, err := pullMessages(r.Context(), client, 10)
		if err != nil {
			log.Printf("failed to pull messages: %v", err)
			http.Error(w, "failed to pull messages", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"messages": messages,
			"count":    len(messages),
		})
	})

	handler := otelhttp.NewHandler(mux, "gonetpath",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

	fmt.Println("Server listening on :3000")
	log.Fatal(http.ListenAndServe(":3000", handler))
}
