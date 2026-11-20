"""Generate deploy/grafana/dashboards/embedsync.json (run: python3 scripts/gen_dashboard.py)."""

import json

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
pid = 0


def panel(title, kind, targets, x, y, w, h, unit=None, extra=None):
    global pid
    pid += 1
    p = {
        "id": pid,
        "title": title,
        "type": kind,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"datasource": DS, "expr": e, "legendFormat": lf, "refId": chr(65 + i)} for i, (e, lf) in enumerate(targets)],
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
    }
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if extra:
        for k, v in extra.items():
            p[k] = v
    panels.append(p)


state_map = {
    "fieldConfig": {
        "defaults": {
            "mappings": [{"type": "value", "options": {"1": {"text": "ON"}, "0": {"text": "-"}}}],
            "color": {"mode": "thresholds"},
            "thresholds": {"mode": "absolute", "steps": [{"color": "transparent", "value": None}, {"color": "green", "value": 1}]},
        },
        "overrides": [],
    },
    "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "textMode": "value_and_name", "colorMode": "background"},
}
panel("Consumer state (idle vs stalled)", "stat", [("embedsync_consumer_state", "{{state}}")], 0, 0, 8, 4, extra=state_map)
panel("Freshness lag (oldest pending or unresolved)", "stat",
      [("embedsync_freshness_lag_seconds", "lag")], 8, 0, 4, 4, "s",
      extra={"options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"},
             "fieldConfig": {"defaults": {"unit": "s", "thresholds": {"mode": "absolute", "steps": [
                 {"color": "green", "value": None}, {"color": "orange", "value": 5}, {"color": "red", "value": 10}]}}, "overrides": []}})
panel("Unresolved (quarantined) rows", "stat", [("sum(embedsync_unresolved_rows) or vector(0)", "open")], 12, 0, 4, 4,
      extra={"options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"},
             "fieldConfig": {"defaults": {"thresholds": {"mode": "absolute", "steps": [
                 {"color": "green", "value": None}, {"color": "red", "value": 1}]}}, "overrides": []}})
panel("Retained WAL", "stat", [("embedsync_slot_retained_wal_bytes", "retained")], 16, 0, 4, 4, "bytes",
      extra={"options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"}})
panel("Serving generation", "stat", [("embedsync_serving_generation == 1", "{{generation}}")], 20, 0, 4, 4,
      extra={"options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "textMode": "name"}})

panel("Commit-to-searchable latency", "timeseries", [
    ("histogram_quantile(0.50, sum(rate(embedsync_commit_to_searchable_seconds_bucket[1m])) by (le))", "p50"),
    ("histogram_quantile(0.99, sum(rate(embedsync_commit_to_searchable_seconds_bucket[1m])) by (le))", "p99"),
    ("embedsync_alert_threshold{name=\"freshness_seconds\"}", "target"),
], 0, 4, 12, 8, "s")
panel("Throughput", "timeseries", [
    ("rate(embedsync_source_transactions_total[1m])", "source txns/s"),
    ("sum(rate(embedsync_documents_applied_total[1m]))", "documents/s"),
    ("sum(rate(embedsync_embeddings_total[1m]))", "embeddings/s"),
    ("sum(rate(embedsync_embeddings_skipped_total[1m]))", "embeddings skipped/s"),
], 12, 4, 12, 8, "ops")
panel("Queue depth and pending work", "timeseries", [
    ("embedsync_queue_depth", "{{stage}}"),
    ("embedsync_pending_transactions", "pending txns"),
], 0, 12, 8, 8)
panel("Retries and embed outcomes", "timeseries", [
    ("sum(rate(embedsync_retries_total[1m])) by (component)", "retry {{component}}"),
    ("sum(rate(embedsync_embed_requests_total[1m])) by (outcome)", "embed {{outcome}}"),
], 8, 12, 8, 8, "ops")
panel("Unresolved rows by generation", "timeseries", [("embedsync_unresolved_rows", "{{generation}}"),
                                                       ("sum(rate(embedsync_quarantined_total[5m])) by (reason)", "quarantined/s {{reason}}")],
      16, 12, 8, 8)
panel("Replication slot WAL", "timeseries", [
    ("embedsync_slot_retained_wal_bytes", "retained (restart_lsn)"),
    ("embedsync_slot_confirmed_lag_bytes", "unconfirmed (confirmed_flush_lsn)"),
    ("embedsync_alert_threshold{name=\"retained_wal_warning_bytes\"}", "warning"),
    ("embedsync_alert_threshold{name=\"retained_wal_critical_bytes\"}", "critical"),
    ("embedsync_slot_safe_wal_bytes >= 0", "safe_wal_size"),
], 0, 20, 12, 8, "bytes")
panel("Idle vs stalled: pending age, progress age, last message", "timeseries", [
    ("embedsync_oldest_pending_commit_age_seconds", "oldest pending commit age"),
    ("embedsync_last_progress_age_seconds", "since checkpoint last advanced"),
    ("embedsync_last_source_message_age_seconds", "since last source message"),
], 12, 20, 12, 8, "s")
panel("Search latency and requests", "timeseries", [
    ("histogram_quantile(0.99, sum(rate(embedsync_search_seconds_bucket[1m])) by (le))", "p99 search"),
    ("histogram_quantile(0.50, sum(rate(embedsync_search_seconds_bucket[1m])) by (le))", "p50 search"),
], 0, 28, 12, 8, "s")
panel("Generations", "table", [("embedsync_generation_info", "")], 12, 28, 12, 8,
      extra={"targets": [{"datasource": DS, "expr": "embedsync_generation_info", "format": "table", "instant": True, "refId": "A"}],
             "transformations": [{"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True, "__name__": True, "instance": True, "job": True}}}]})

dash = {
    "uid": "embedsync",
    "title": "embedsync — freshness and slot safety",
    "tags": ["embedsync", "pgvector", "cdc"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5s",
    "time": {"from": "now-30m", "to": "now"},
    "panels": panels,
}
with open("deploy/grafana/dashboards/embedsync.json", "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print(f"{len(panels)} panels")
