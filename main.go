package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"os"

	"cloud.google.com/go/pubsub/v2"
	pubsubapi "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	projectID      = requireEnv("GCP_PROJECT_ID")
	topicID        = requireEnv("GCP_PUBSUB_TOPIC_ID")
	subscriptionID = requireEnv("GCP_PUBSUB_SUBSCRIPTION_ID")
)

var tracer = otel.Tracer("gonetpath")

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required environment variable %s is not set", key)
	}
	return v
}

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

func pullMessages(ctx context.Context, admin *pubsubapi.SubscriptionAdminClient, batchSize int) ([]string, error) {
	ctx, span := tracer.Start(ctx, "pubsub.pull",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	subscriptionPath := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subscriptionID)

	span.SetAttributes(
		attribute.String("peer.service", "google-cloud-pubsub"),
		attribute.String("server.address", "pubsub.googleapis.com"),
		attribute.String("messaging.system", "gcp_pubsub"),
		attribute.String("messaging.destination.name", subscriptionID),
		attribute.String("messaging.operation.name", "receive"),
	)

	drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	defer drainCancel()

	var (
		messages []string
		batches  int
	)

	for drainCtx.Err() == nil {
		pullCtx, pullCancel := context.WithTimeout(drainCtx, 5*time.Second)
		resp, err := admin.Pull(pullCtx, &pubsubpb.PullRequest{
			Subscription: subscriptionPath,
			MaxMessages:  int32(batchSize),
		})
		pullCancel()
		if err != nil {
			// A per-call deadline with no messages means the subscription is drained.
			if status.Code(err) == gcodes.DeadlineExceeded {
				break
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return messages, err
		}

		received := resp.GetReceivedMessages()
		if len(received) == 0 {
			break
		}

		batches++
		ackIDs := make([]string, 0, len(received))
		for _, rm := range received {
			if m := rm.GetMessage(); m != nil {
				messages = append(messages, string(m.GetData()))
			}
			if id := rm.GetAckId(); id != "" {
				ackIDs = append(ackIDs, id)
			}
		}

		if len(ackIDs) > 0 {
			if err := acknowledgeMessages(ctx, admin, subscriptionPath, ackIDs); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return messages, err
			}
		}
	}

	span.SetAttributes(
		attribute.Int("messaging.batch.message_count", len(messages)),
		attribute.Int("pubsub.pull.batches", batches),
	)
	return messages, nil
}

func acknowledgeMessages(ctx context.Context, admin *pubsubapi.SubscriptionAdminClient, subscriptionPath string, ackIDs []string) error {
	ctx, span := tracer.Start(ctx, "pubsub.ack",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()
	span.SetAttributes(
		attribute.String("peer.service", "google-cloud-pubsub"),
		attribute.String("server.address", "pubsub.googleapis.com"),
		attribute.String("messaging.system", "gcp_pubsub"),
		attribute.String("messaging.destination.name", subscriptionID),
		attribute.String("messaging.operation.name", "ack"),
		attribute.Int("messaging.batch.message_count", len(ackIDs)),
	)

	ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := admin.Acknowledge(ackCtx, &pubsubpb.AcknowledgeRequest{
		Subscription: subscriptionPath,
		AckIds:       ackIDs,
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
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

	adminClient, err := pubsubapi.NewSubscriptionAdminClient(ctx,
		option.WithGRPCDialOption(grpc.WithStatsHandler(otelgrpc.NewClientHandler())),
	)
	if err != nil {
		log.Fatalf("failed to create pubsub subscription admin client: %v", err)
	}
	defer adminClient.Close()

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

		messages, err := pullMessages(r.Context(), adminClient, 10)
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
