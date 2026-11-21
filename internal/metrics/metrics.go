// Package metrics defines the Prometheus metrics exported by embedsync.
//
// Labels never carry row content or keys: only generation ids, stages,
// outcomes and reasons drawn from fixed sets.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the process registry.
var Registry = prometheus.NewRegistry()

var (
	// CommitToSearchable is the latency from source commit to sink commit.
	CommitToSearchable = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "embedsync_commit_to_searchable_seconds",
		Help:    "Time from source transaction commit to the sink transaction that makes its effects searchable.",
		Buckets: []float64{0.025, 0.05, 0.1, 0.2, 0.35, 0.5, 0.75, 1, 1.5, 2, 3, 5, 7.5, 10, 15, 30, 60, 120, 300},
	})
	// SourceTransactions counts applied source transactions.
	SourceTransactions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "embedsync_source_transactions_total", Help: "Source transactions durably applied."})
	// Changes counts decoded row changes by operation.
	Changes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_changes_total", Help: "Decoded source row changes by operation."}, []string{"op"})
	// DocsApplied counts document effects written per generation and origin.
	DocsApplied = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_documents_applied_total", Help: "Document effects written to the sink."}, []string{"generation", "origin", "op"})
	// Embeddings counts vectors computed by the embedding service.
	Embeddings = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_embeddings_total", Help: "Chunks embedded by the embedding service."}, []string{"generation"})
	// EmbeddingsSkipped counts chunks whose vector was reused.
	EmbeddingsSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_embeddings_skipped_total", Help: "Chunks whose fingerprint was unchanged or already embedded."}, []string{"generation", "source"})
	// EmbedRequests counts embedding HTTP requests by outcome.
	EmbedRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_embed_requests_total", Help: "Embedding service requests by outcome."}, []string{"outcome"})
	// EmbedRequestSeconds is request latency.
	EmbedRequestSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "embedsync_embed_request_seconds", Help: "Embedding request latency.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12)})
	// ProviderRequests counts embedding requests per provider and outcome
	// (ok, rejected, throttled, unavailable, invalid, error).
	ProviderRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_provider_requests_total", Help: "Embedding requests per provider and outcome."}, []string{"provider", "outcome"})
	// ProviderRequestSeconds is per-provider request latency.
	ProviderRequestSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "embedsync_provider_request_seconds", Help: "Embedding request latency per provider.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}}, []string{"provider"})
	// ProviderRetries counts retries per provider and reason; ProviderRetryWait sums the waits.
	ProviderRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_provider_retries_total", Help: "Embedding retries per provider and reason (throttled, unavailable, invalid)."}, []string{"provider", "reason"})
	ProviderRetryWait = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_provider_retry_wait_seconds_total", Help: "Time spent waiting before embedding retries (incl. Retry-After)."}, []string{"provider"})
	// ProviderInputs counts inputs embedded per provider.
	ProviderInputs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_provider_inputs_total", Help: "Inputs embedded per provider."}, []string{"provider"})
	// ProviderTokens counts tokens per provider; source is reported (by the
	// provider) or estimated (input bytes / 4).
	ProviderTokens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_provider_tokens_total", Help: "Embedding tokens per provider (reported by the provider or estimated)."}, []string{"provider", "source"})
	// ProviderCost is an ESTIMATED cost: tokens x the configured price,
	// labelled with the pricing source the price was taken from.
	ProviderCost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_provider_estimated_cost_usd_total", Help: "ESTIMATED embedding cost in USD (tokens x configured price per million); see pricing_source."},
		[]string{"provider", "pricing_source"})
	// Retries counts retried operations.
	Retries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_retries_total", Help: "Retried operations by component."}, []string{"component"})
	// Quarantined counts rows moved to the DLQ.
	Quarantined = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_quarantined_total", Help: "Row versions quarantined."}, []string{"reason"})
	// QueueDepth is the depth of bounded internal queues.
	QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "embedsync_queue_depth", Help: "Items waiting in bounded internal queues."}, []string{"stage"})
	// PendingTransactions are received but not yet durable.
	PendingTransactions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_pending_transactions", Help: "Source transactions received but not yet durable in the sink."})
	// OldestPendingAge is the age of the oldest non-durable commit.
	OldestPendingAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_oldest_pending_commit_age_seconds", Help: "Age of the oldest received source commit not yet searchable (0 when idle)."})
	// FreshnessLag combines pending and unresolved work.
	FreshnessLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_freshness_lag_seconds", Help: "Max of oldest pending commit age and oldest unresolved (quarantined) row age."})
	// CheckpointLSN is the durable checkpoint.
	CheckpointLSN = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_checkpoint_lsn", Help: "Durable checkpoint LSN as a number."})
	// ServerWALEnd is the last server WAL end seen.
	ServerWALEnd = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_server_wal_end_lsn", Help: "Server WAL end from the last replication message."})
	// RetainedWAL is WAL retained by the slot (current - restart_lsn).
	RetainedWAL = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_slot_retained_wal_bytes", Help: "WAL bytes the replication slot forces the source to keep."})
	// ConfirmedLag is current WAL minus confirmed_flush_lsn.
	ConfirmedLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_slot_confirmed_lag_bytes", Help: "Bytes between the source WAL head and the slot's confirmed_flush_lsn."})
	// SlotSafeWAL is safe_wal_size (bytes before the slot may be invalidated), -1 when unlimited.
	SlotSafeWAL = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_slot_safe_wal_bytes", Help: "Bytes that can still be written before the slot risks invalidation (-1 when max_slot_wal_keep_size is unlimited)."})
	// SlotActive is 1 when the slot has an active consumer.
	SlotActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_slot_active", Help: "1 when the replication slot is active."})
	// SlotWALStatus is one-hot by wal_status.
	SlotWALStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "embedsync_slot_wal_status", Help: "One-hot pg_replication_slots.wal_status."}, []string{"status"})
	// Unresolved is open DLQ rows per generation.
	Unresolved = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "embedsync_unresolved_rows", Help: "Open quarantined row versions per generation."}, []string{"generation"})
	// ConsumerState is one-hot idle/active/stalled/paused.
	ConsumerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "embedsync_consumer_state", Help: "One-hot consumer state: idle, active, stalled, paused, starting."}, []string{"state"})
	// LastMessageAge is the age of the last message from the source.
	LastMessageAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_last_source_message_age_seconds", Help: "Seconds since the last replication message (keepalive or data)."})
	// LastProgressAge is the time since durable progress last advanced.
	LastProgressAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "embedsync_last_progress_age_seconds", Help: "Seconds since the durable checkpoint last advanced (stall detection)."})
	// SinkCommitSeconds is the duration of sink transactions.
	SinkCommitSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "embedsync_sink_transaction_seconds", Help: "Duration of sink apply transactions.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14)})
	// BackfillRows counts snapshot rows emitted.
	BackfillRows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_backfill_rows_total", Help: "Snapshot rows read by watermark backfill."}, []string{"generation"})
	// BackfillWindows counts completed and discarded windows.
	BackfillWindows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_backfill_windows_total", Help: "Backfill windows by outcome."}, []string{"outcome"})
	// GenerationInfo exposes state per generation.
	GenerationInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "embedsync_generation_info", Help: "1 for each generation and its current state."}, []string{"generation", "state", "model"})
	// Serving is 1 for the serving generation.
	Serving = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "embedsync_serving_generation", Help: "1 for the generation that answers searches."}, []string{"generation"})
	// AlertThreshold publishes configured thresholds for dashboards and rules.
	AlertThreshold = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "embedsync_alert_threshold", Help: "Configured alert thresholds."}, []string{"name"})
	// SearchRequests counts search requests.
	SearchRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "embedsync_search_requests_total", Help: "Search requests by generation and outcome."}, []string{"generation", "outcome"})
	// SearchSeconds is search latency.
	SearchSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "embedsync_search_seconds", Help: "End-to-end search latency including query embedding.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12)})
)

func init() {
	Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		CommitToSearchable, SourceTransactions, Changes, DocsApplied, Embeddings, EmbeddingsSkipped, EmbedRequests,
		EmbedRequestSeconds, ProviderRequests, ProviderRequestSeconds, ProviderRetries, ProviderRetryWait, ProviderInputs, ProviderTokens, ProviderCost, Retries, Quarantined, QueueDepth, PendingTransactions, OldestPendingAge, FreshnessLag,
		CheckpointLSN, ServerWALEnd, RetainedWAL, ConfirmedLag, SlotSafeWAL, SlotActive, SlotWALStatus, Unresolved,
		ConsumerState, LastMessageAge, LastProgressAge, SinkCommitSeconds, BackfillRows, BackfillWindows, GenerationInfo, Serving,
		AlertThreshold, SearchRequests, SearchSeconds)
}

// SetConsumerState sets the one-hot consumer state.
func SetConsumerState(state string) {
	for _, s := range []string{"starting", "idle", "active", "stalled", "paused"} {
		v := 0.0
		if s == state {
			v = 1
		}
		ConsumerState.WithLabelValues(s).Set(v)
	}
}
