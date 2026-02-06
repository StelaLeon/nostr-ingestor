# Nostr Datalake Pipelines

```txt
┌─────────────────────────────────────────────────────────────────────┐
│                           Data Pipeline                              │
│                                                                       │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐                │
│  │   Relay 1  │    │   Relay 2  │    │   Relay N  │                │
│  │ WebSocket  │    │ WebSocket  │    │ WebSocket  │                │
│  └─────┬──────┘    └─────┬──────┘    └─────┬──────┘                │
│        │                  │                  │                        │
│        ├──────────────────┴──────────────────┘                        │
│        │                                                               │
│        v                                                               │
│  ┌─────────────────────────────────────────────────┐                │
│  │          Event Processing Layer                  │                │
│  │  • Parse Nostr messages                          │                │
│  │  • Check cache for duplicates                    │                │
│  │  • Apply subscription update strategy            │                │
│  │  • Transform events to output format             │                │
│  └─────────────┬───────────────────────────────────┘                │
│                │                                                       │
│                v                                                       │
│  ┌─────────────────────────────────────────────────┐                │
│  │            Batching & Sink Layer                 │                │
│  │  • Group events by size/time window              │                │
│  │  • Parallel writes to BigQuery                   │                │
│  │  • Error recovery and retry logic                │                │
│  └──────────────────────────────────────────────────┘                │
│                │                                                       │
│                v                                                       │
│         [BigQuery Table]                                              │
└───────────────────────────────────────────────────────────────────────┘

```

### Built-in strategies:
```
class TimeWindowUpdateStrategy[T <: NostrFilter](
  eventThreshold: Int = 100,
  timeWindowSeconds: Long = 3600
)
```

This prevents unbounded accumulation by periodically updating the subscription's since filter.
