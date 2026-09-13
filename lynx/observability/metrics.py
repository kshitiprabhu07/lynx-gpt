from prometheus_client import Counter, Gauge, Histogram, make_asgi_app

# One metric per row of the HLD Section 12 failure table.
fetch_failures = Counter("lynx_fetch_failures_total",
                         "Fetch failures", ["source_id", "reason"])
source_last_success = Gauge("lynx_source_last_success_ts",
                            "Last successful fetch", ["source_id"])
field_completeness = Gauge("lynx_field_completeness_ratio",
                           "Selector drift signal", ["source_id"])
zero_item_extractions = Counter("lynx_zero_item_extractions_total",
                                "Zero-item anomaly", ["source_id"])
quarantine_depth = Gauge("lynx_quarantine_depth", "Records in quarantine")
queue_oldest_age = Gauge("lynx_queue_oldest_pending_age_seconds",
                         "Queue backlog age")
dlq_depth = Gauge("lynx_dead_letter_depth", "Dead-lettered messages")
index_lag = Gauge("lynx_index_projection_lag_seconds", "Indexer lag")
delivery_status = Counter("lynx_notification_deliveries_total",
                          "Deliveries", ["status"])
redis_errors = Counter("lynx_redis_errors_total", "Redis errors", ["namespace"])
cache_hit_ratio = Gauge("lynx_cache_hit_ratio", "RAG cache hit ratio")
rate_limit_rejections = Counter("lynx_rate_limit_rejections_total",
                                "429s issued", ["namespace"])
request_latency = Histogram("lynx_request_duration_seconds",
                            "Request latency", ["route"])


def install_metrics(app):
    app.mount("/metrics", make_asgi_app())