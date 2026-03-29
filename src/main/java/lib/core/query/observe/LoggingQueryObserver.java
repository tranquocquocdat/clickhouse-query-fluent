package lib.core.query.observe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in {@link QueryObserver} that logs query events via SLF4J
 * with structured, easy-to-read output.
 *
 * <p>
 * Activated automatically via Spring Boot AutoConfiguration when
 * {@code clickhouse-query.logging.enabled=true} is present in
 * {@code application.yml}.
 *
 * <h3>Log format examples</h3>
 * 
 * <pre>
 * INFO  🔍 [CH] LIST  | MISS   |  843ms | rows=20 | page=0/20
 * INFO  ✅ [CH] PAGE  | HIT    |    2ms | rows=20 | page=0/20
 * INFO  💾 [CH] LIST  | MISS   |  843ms | saved to cache (async)
 * DEBUG     sql  : SELECT date, sum(revenue) AS total FROM orders WHERE tenant_id = :tenantId
 * DEBUG     params: {tenantId=TENANT_001, fromDate=2026-01-01}
 * WARN  ⚠️ [CH-SLOW] LIST | MISS | 2341ms | rows=10000
 * WARN     ↳ sql  : SELECT ...
 * WARN     ↳ params: {tenantId=TENANT_001}
 * </pre>
 */
public final class LoggingQueryObserver implements QueryObserver {

    private static final Logger log = LoggerFactory.getLogger("clickhouse-query");

    // ── ANSI-free visual markers (work in all log viewers) ─────────────────
    private static final String ICON_QUERY = "🔍";
    private static final String ICON_HIT = "✅";
    private static final String ICON_MISS = "💾";
    private static final String ICON_SLOW = "⚠️ ";
    private static final String PREFIX = "[CH]";

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

    /** Log cache HIT / MISS status at INFO level. */
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

        // ── Slow-query alert (WARN) — always logged regardless of logSql ──
        if (slow) {
            log.warn("{} [CH-SLOW] {:<4} | {:<8} | {}ms | rows={}{}",
                    ICON_SLOW,
                    e.getQueryType(),
                    e.getCacheStatus(),
                    e.getDurationMs(),
                    e.getResultCount(),
                    pageInfo(e));
            if (e.getSql() != null) {
                log.warn("     ↳ sql   : {}", e.getSql());
            }
            if (logParams && e.getParams() != null) {
                log.warn("     ↳ params: {}", e.getParams());
            }
            return;
        }

        // ── Cache metrics (INFO) ──
        if (trackMetrics && log.isInfoEnabled()) {
            switch (e.getCacheStatus()) {
                case HIT ->
                    log.info("{} {} {:<5} | {:<8} | {:>5}ms | rows={}{}",
                            ICON_HIT, PREFIX,
                            e.getQueryType(), e.getCacheStatus(),
                            e.getDurationMs(), e.getResultCount(),
                            pageInfo(e));
                case MISS ->
                    log.info("{} {} {:<5} | {:<8} | {:>5}ms | rows={}{} — saved to cache (async)",
                            ICON_MISS, PREFIX,
                            e.getQueryType(), e.getCacheStatus(),
                            e.getDurationMs(), e.getResultCount(),
                            pageInfo(e));
                case DISABLED -> {
                    if (logSql && log.isDebugEnabled()) {
                        log.debug("{} {} {:<5} | {:<8} | {:>5}ms | rows={}{}",
                                ICON_QUERY, PREFIX,
                                e.getQueryType(), e.getCacheStatus(),
                                e.getDurationMs(), e.getResultCount(),
                                pageInfo(e));
                    }
                }
            }
        } else if (logSql && log.isDebugEnabled()) {
            // metrics disabled — still log at DEBUG when logSql=true
            log.debug("{} {} {:<5} | {:<8} | {:>5}ms | rows={}{}",
                    ICON_QUERY, PREFIX,
                    e.getQueryType(), e.getCacheStatus(),
                    e.getDurationMs(), e.getResultCount(),
                    pageInfo(e));
        }

        // ── SQL + params detail (DEBUG) — only on MISS or DISABLED (not HIT, no DB
        // call) ──
        if (logSql && log.isDebugEnabled()
                && e.getCacheStatus() != CacheStatus.HIT
                && e.getSql() != null) {
            log.debug("     sql   : {}", e.getSql());
            if (logParams && e.getParams() != null) {
                log.debug("     params: {}", e.getParams());
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static String pageInfo(QueryEvent e) {
        return e.getPage() >= 0
                ? String.format(" | page=%d/%d", e.getPage(), e.getPageSize())
                : "";
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

        /** Also log bind parameter values (may expose PII). Default: false. */
        public Builder logParams(boolean v) {
            this.logParams = v;
            return this;
        }

        /** Queries above this threshold → logged at WARN. Default: 1000ms. */
        public Builder slowQueryThresholdMs(long ms) {
            this.slowQueryThresholdMs = ms;
            return this;
        }

        /** Log cache HIT / MISS at INFO. Default: true. */
        public Builder trackMetrics(boolean v) {
            this.trackMetrics = v;
            return this;
        }

        public LoggingQueryObserver build() {
            return new LoggingQueryObserver(this);
        }
    }
}
