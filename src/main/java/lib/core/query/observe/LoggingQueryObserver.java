package lib.core.query.observe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in {@link QueryObserver} that logs query events via SLF4J.
 *
 * <p>
 * Configure once at startup and register through {@link QueryObserverRegistry}:
 *
 * <pre>
 * {@code
 * // application.yml
 * clickhouse-query:
 *   logging:
 *     enabled: true
 *     log-sql: true
 *     log-params: false        # set true to log bind parameters (can expose PII)
 *     slow-query-ms: 1000      # log WARN when query exceeds this threshold
 *   metrics:
 *     enabled: true            # log cache hit/miss stats
 *
 * // Spring @Configuration
 * &#64;Bean
 * &#64;ConditionalOnProperty("clickhouse-query.logging.enabled")
 * public QueryObserver clickHouseQueryObserver(
 *         &#64;Value("${clickhouse-query.logging.log-sql:true}") boolean logSql,
 *         &#64;Value("${clickhouse-query.logging.log-params:false}") boolean logParams,
 *         &#64;Value("${clickhouse-query.logging.slow-query-ms:1000}") long slowQueryMs,
 *         &#64;Value("${clickhouse-query.metrics.enabled:true}") boolean metrics) {
 *
 *     LoggingQueryObserver obs = LoggingQueryObserver.builder()
 *         .logSql(logSql)
 *         .logParams(logParams)
 *         .slowQueryThresholdMs(slowQueryMs)
 *         .trackMetrics(metrics)
 *         .build();
 *     QueryObserverRegistry.register(obs);
 *     return obs;
 * }
 * }
 * </pre>
 */
public final class LoggingQueryObserver implements QueryObserver {

    private static final Logger log = LoggerFactory.getLogger("clickhouse-query");

    // ── Configuration ──────────────────────────────────────────────────────

    /** Log the SQL of every query at DEBUG level. */
    private final boolean logSql;

    /** Log bind parameters (values) alongside SQL at DEBUG level. */
    private final boolean logParams;

    /**
     * Queries taking longer than this threshold (ms) are logged at WARN level.
     * Set to {@code Long.MAX_VALUE} to disable slow-query logging.
     */
    private final long slowQueryThresholdMs;

    /** Log cache HIT / MISS / DISABLED status. */
    private final boolean trackMetrics;

    private LoggingQueryObserver(Builder b) {
        this.logSql = b.logSql;
        this.logParams = b.logParams;
        this.slowQueryThresholdMs = b.slowQueryThresholdMs;
        this.trackMetrics = b.trackMetrics;
    }

    // ── QueryObserver ──────────────────────────────────────────────────────

    @Override
    public void onQuery(QueryEvent e) {
        boolean slow = e.getDurationMs() >= slowQueryThresholdMs;

        // ── Slow-query alert (WARN) ──
        if (slow) {
            log.warn("[SLOW QUERY] {}ms | type={} | cache={} | rows={} | sql={}",
                    e.getDurationMs(), e.getQueryType(), e.getCacheStatus(),
                    e.getResultCount(), e.getSql());
            if (logParams && e.getParams() != null) {
                log.warn("[SLOW QUERY params] {}", e.getParams());
            }
            return; // already logged at WARN, skip DEBUG
        }

        // ── Normal query (DEBUG) ──
        if (logSql && log.isDebugEnabled()) {
            log.debug("[{}] {}ms | cache={} | rows={} | sql={}",
                    e.getQueryType(), e.getDurationMs(),
                    e.getCacheStatus(), e.getResultCount(), e.getSql());

            if (logParams && e.getParams() != null) {
                log.debug("[params] {}", e.getParams());
            }
        }

        // ── Metrics / cache stats (INFO) ──
        if (trackMetrics && log.isInfoEnabled()) {
            switch (e.getCacheStatus()) {
                case HIT -> log.info("[Cache HIT]  type={} | {}ms | rows={}",
                        e.getQueryType(), e.getDurationMs(), e.getResultCount());
                case MISS -> log.info("[Cache MISS] type={} | {}ms | rows={} — saved to cache",
                        e.getQueryType(), e.getDurationMs(), e.getResultCount());
                case DISABLED -> {
                    /* no cache → skip metrics log */ }
            }
        }
    }

    // ── Builder ────────────────────────────────────────────────────────────

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean logSql = true;
        private boolean logParams = false;
        private long slowQueryThresholdMs = 1_000L;
        private boolean trackMetrics = true;

        /** Log every SQL statement at DEBUG level. */
        public Builder logSql(boolean v) {
            this.logSql = v;
            return this;
        }

        /**
         * Also log the bind parameter values (may expose PII — disabled by default).
         */
        public Builder logParams(boolean v) {
            this.logParams = v;
            return this;
        }

        /**
         * Queries slower than this threshold will be logged at WARN. Default: 1000ms.
         */
        public Builder slowQueryThresholdMs(long ms) {
            this.slowQueryThresholdMs = ms;
            return this;
        }

        /** Log cache HIT / MISS at INFO level. Default: true. */
        public Builder trackMetrics(boolean v) {
            this.trackMetrics = v;
            return this;
        }

        public LoggingQueryObserver build() {
            return new LoggingQueryObserver(this);
        }
    }
}
