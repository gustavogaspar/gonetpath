# Session Compact — gonetpath

## Primary Request and Intent

The user built a Go service (`gonetpath`) that:
- Exposes a REST API on port 3000 with POST `/publish` and GET `/pull` endpoints
- Publishes/consumes messages from Google Cloud Pub/Sub (topic, subscription, and project configured via env vars)
- Is instrumented with OpenTelemetry (no Datadog SDK, no USM, no auto-instrumentation) sending traces to Datadog via the local Datadog Agent OTLP receiver
- Should appear in Datadog's External Services / External Provider Status list
- Uses environment variables for all GCP config (`GCP_PROJECT_ID`, `GCP_PUBSUB_TOPIC_ID`, `GCP_PUBSUB_SUBSCRIPTION_ID`)

---

## Files

### `main.go`
- HTTP server on `:3000`
- `/publish` (POST) — publishes a `{"message":"text"}` body to Pub/Sub with a PRODUCER span
- `/pull` (GET) — pulls up to 10 messages from Pub/Sub with a CONSUMER span
- OTel attributes: `peer.service`, `server.address`, `messaging.system`, `messaging.destination.name`, `messaging.operation.name`
- gRPC OTel instrumentation injected into PubSub SDK via `option.WithGRPCDialOption(grpc.WithStatsHandler(otelgrpc.NewClientHandler()))`
- HTTP handler wrapped with `otelhttp.NewHandler`
- Config driven by `requireEnv()` helper (fatal if unset)

### `telemetry.go`
- Reads `OTEL_EXPORTER_OTLP_ENDPOINT` (defaults to `localhost:4317`)
- Sets `service.name=gonetpath`, `deployment.environment=dev`
- OTLP gRPC exporter with insecure transport

### `go.mod`
- Module: `gonetpath`, go 1.25.4
- Key deps: `cloud.google.com/go/pubsub/v2`, `go.opentelemetry.io/otel`, `otelhttp`, `otelgrpc`, `otlptracegrpc`, `google.golang.org/api/option`, `google.golang.org/grpc`

### `/etc/datadog-agent/datadog.yaml` (modified)
```yaml
otlp_config:
  receiver:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"

apm_config:
  compute_stats_by_span_kind: true
  peer_tags_aggregation: true
  features:
    - "enable_otlp_compute_top_level_by_span_kind"
```

---

## Datadog External Provider Status — Required Attributes

### Why messaging spans alone are not enough

Datadog's **External Provider Status** (the cloud provider card in APM > External Services) is built from **CLIENT-kind spans** that carry `rpc.*` or `http.*` attributes pointing to an external host. PRODUCER and CONSUMER spans feed the **Messaging** view, not the External Provider view. Both views can coexist in a single trace, but they require different span kinds.

### What the Agent needs (datadog.yaml)

```yaml
apm_config:
  compute_stats_by_span_kind: true       # default true since v7.60
  peer_tags_aggregation: true            # default true since v7.60
  features:
    - "enable_otlp_compute_top_level_by_span_kind"  # still requires explicit opt-in
```

`enable_otlp_compute_top_level_by_span_kind` tells the Agent to treat OTel SERVER and CONSUMER spans as top-level (entry points) so they generate metrics. Without it, only Datadog-native spans are promoted to top-level.

### Span kind matrix

| Span kind | What it feeds in Datadog | Needs `rpc.*`/`http.*`? |
|---|---|---|
| `SERVER` | Inbound service entry (your service's own metrics) | No |
| `CLIENT` | **External Services list** | Yes — `server.address` or `rpc.service` |
| `PRODUCER` | Messaging > Produce view | No |
| `CONSUMER` | Messaging > Consume view | No |

### Minimum attributes for a CLIENT span to appear in External Provider Status

```
span.kind          = CLIENT               (mandatory)
server.address     = <hostname>           (e.g. "pubsub.googleapis.com")
rpc.system         = grpc                 (for gRPC calls)
rpc.service        = <proto service>      (e.g. "google.pubsub.v1.Publisher")
rpc.method         = <method name>        (e.g. "Publish")
peer.service       = <logical name>       (e.g. "google-cloud-pubsub")
```

`peer.service` becomes the display name in the External Services card. If omitted, Datadog falls back to `server.address`.

### Minimum attributes for messaging spans (Messaging view)

```
span.kind                        = PRODUCER | CONSUMER
messaging.system                 = gcp_pubsub | kafka | rabbitmq | ...
messaging.destination.name       = <topic or queue name>
messaging.operation.name         = publish | receive | process | ...
messaging.message.body.size      = <bytes>           (optional but recommended)
messaging.batch.message_count    = <int>             (optional, for batch ops)
server.address                   = pubsub.googleapis.com
peer.service                     = <logical name>
```

### How this project generates both

The PubSub SDK makes internal gRPC calls. By injecting `otelgrpc.NewClientHandler()` as a gRPC stats handler, every SDK call automatically emits a CLIENT span with `rpc.system`, `rpc.service`, `rpc.method`, and `server.address` — no manual attribute setting required. The manual PRODUCER/CONSUMER spans in `publishMessage` and `pullMessages` feed the Messaging view on top of that.

```go
// This single line makes the SDK emit CLIENT spans with rpc.* attributes:
option.WithGRPCDialOption(grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
```

### Attribute propagation to Datadog tags

Datadog maps OTel attributes to its own tag system:

| OTel attribute | Datadog tag |
|---|---|
| `server.address` | `out.host` |
| `rpc.service` | `rpc.service` |
| `rpc.method` | `rpc.method` |
| `peer.service` | `peer.service` (used for External Services display name) |
| `messaging.system` | `messaging.system` |
| `messaging.destination.name` | `messaging.destination` |
| `http.response.status_code` | `http.status_code` |

---

## Key Technical Decisions

| Decision | Reason |
|---|---|
| `pubsub/v2` uses `client.Subscriber()` not `client.Subscription()` | v2 API change from v1 |
| Inject `otelgrpc.NewClientHandler()` into PubSub gRPC transport | Datadog External Provider Status requires gRPC CLIENT spans with `rpc.*` attributes, not just messaging PRODUCER/CONSUMER spans |
| `option.WithGRPCDialOption(grpc.WithStatsHandler(...))` | Only way to pass gRPC dial options into GCP client libraries |
| `enable_otlp_compute_top_level_by_span_kind` feature flag | Needed for OTel span kinds to be properly recognized even though `compute_stats_by_span_kind` is default-on at v7.60+ |

---

## Errors Encountered and Fixed

- **`client.Subscription undefined`**: v2 API uses `client.Subscriber()` — fixed.
- **Datadog Agent failed after `otlp_config` added**: `sudo tee -a` changed file ownership; `system-probe.yaml` wasn't readable by `dd-agent`. Fixed: `sudo chown root:dd-agent /etc/datadog-agent/system-probe.yaml && sudo chmod 640`.
- **Port 3000 already in use**: Old process still running. Fixed: `fuser -k 3000/tcp` before restart.
- **Background server exiting immediately**: Using both `run_in_background: true` AND `&` caused issues. Fixed: explicit `> /tmp/gonetpath.log 2>&1 &` without the flag.
- **External Services not appearing**: PRODUCER/CONSUMER spans alone are insufficient; needed actual gRPC CLIENT spans. Fixed by injecting `otelgrpc` into the PubSub SDK transport.

---

## How to Run

```bash
export GCP_PROJECT_ID=<your-project-id>
export GCP_PUBSUB_TOPIC_ID=<your-topic-id>
export GCP_PUBSUB_SUBSCRIPTION_ID=<your-subscription-id>
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317

cd /home/ec2-user/projetos/gonetpath
go run . > /tmp/gonetpath.log 2>&1 &
```

Test:
```bash
curl -s -X POST http://localhost:3000/publish -H 'Content-Type: application/json' -d '{"message":"hello"}'
curl -s http://localhost:3000/pull
```
