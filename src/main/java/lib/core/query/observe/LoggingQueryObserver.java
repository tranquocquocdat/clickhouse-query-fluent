package lib.core.query.observe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in {@link QueryObserver} that logs query events via SLF4J
 * using a structured, visually clear format.
 *
 * <h3>Log format</h3>
 * 
 * <pre>
 * ── Normal query (DEBUG / INFO) ──────────────────────────────────────────
 * ✅  [CH] PAGE  │ HIT      │    2ms │ rows=20 │ page=0/20
 * 💾  [CH] LIST  │ MISS     │  843ms │ rows=31
 *    ├─ sql   : SELECT date, sum(revenue) FROM orders WHERE tenant_id = :t
 *    └─ params: {tenantId=TENANT_001, fromDate=2026-01-01}
 * 🔍  [CH] ONE   │ DISABLED │   45ms │ rows=1
 *    └─ sql   : SELECT * FROM orders WHERE id = :id LIMIT 1
 *
 * ── Slow query (WARN) ────────────────────────────────────────────────────
 * ┌─── ⚠️  SLOW QUERY ───────────────────────────────────────────────────
 *   type    : LIST            cache    : MISS
 *   duration: 2341ms          rows     : 10000
 *   sql     : SELECT date, sum(revenue) FROM orders WHERE ...
 *   params  : {tenantId=TENANT_001, fromDate=2026-01-01}
 * └──────────────────────────────────────────────────────────────────────
 * </pre>
 */
public final class LoggingQueryObserver implements QueryObserver {

    private static final Logger log = LoggerFactory.getLogger("clickhouse-query");

    // ── Visual markers ─────────────────────────────────────────────────────
    private static final String PREFIX = "[CH]";
    private static final String ICON_HIT = "✅ ";
    private static final String ICON_MISS = "💾 ";
    private static final String ICON_QERY = "🔍 ";
    private static final String SEP = "│";
    private static final String BOX_TOP = "┌─── ⚠️  SLOW QUERY ──────────────────────────────────────────────────";
    private static final String BOX_BOT = "└──────────────────────────────────────────────────────────────────────";

    // ── Configuration ──────────────────────────────────────────────────────

    private final boolean logSql;
    private final boolean logParams;
    private final long slowQueryThresholdMs;
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
        if (e.getDurationMs() >= slowQueryThresholdMs) {
            logSlow(e);
            return;
        }
        logNormal(e);
    }

    // ── Slow query — box style (WARN) ──────────────────────────────────────

    private void logSlow(QueryEvent e) {
        log.warn(BOX_TOP);
        log.warn("  type    : {:<12}  cache    : {}", e.getQueryType(), e.getCacheStatus());
        log.warn("  duration: {}ms{}          rows     : {}", e.getDurationMs(), padding(e.getDurationMs()),
                e.getResultCount());
        if (e.getPage() >= 0) {
            log.warn("  page    : {}/{}", e.getPage(), e.getPageSize());
        }
        if (e.getSql() != null) {
            log.warn("  sql     : {}", e.getSql());
        }
        if (logParams && e.getParams() != null) {
            log.warn("  params  : {}", e.getParams());
        }
        log.warn(BOX_BOT);
    }

    // ── Normal query — single-line + tree detail (INFO / DEBUG) ───────────

    private void logNormal(QueryEvent e) {
        String icon;
        boolean isHit = e.getCacheStatus() == CacheStatus.HIT;
        boolean isMiss = e.getCacheStatus() == CacheStatus.MISS;

        if (isHit)
            icon = ICON_HIT;
        else if (isMiss)
            icon = ICON_MISS;
        else
            icon = ICON_QERY;

        // ── Main line ──
        String mainLine = String.format("%s %s %-5s %s %-8s %s %5dms %s rows=%d%s",
                icon, PREFIX,
                e.getQueryType(), SEP,
                e.getCacheStatus(), SEP,
                e.getDurationMs(), SEP,
                e.getResultCount(),
                pageInfo(e));

        String cacheSuffix = isMiss ? " — saved to cache (async)" : "";

        if (trackMetrics && (isHit || isMiss)) {
            log.info("{}{}", mainLine, cacheSuffix);
        } else if (logSql && log.isDebugEnabled()) {
            log.debug("{}{}", mainLine, cacheSuffix);
        }

        // ── SQL + params — only when DB was queried (not HIT) ──
        if (!isHit && logSql && log.isDebugEnabled() && e.getSql() != null) {
            if (logParams && e.getParams() != null) {
                log.debug("   ├─ sql   : {}", e.getSql());
                log.debug("   └─ params: {}", e.getParams());
            } else {
                log.debug("   └─ sql   : {}", e.getSql());
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static String pageInfo(QueryEvent e) {
        return e.getPage() >= 0
                ? String.format(" %s page=%d/%d", SEP, e.getPage(), e.getPageSize())
                : "";
    }

    /** pads short durations so the "rows" column aligns better */
    private static String padding(long ms) {
        if (ms < 10)
            return "    ";
        if (ms < 100)
            return "   ";
        if (ms < 1000)
            return "  ";
        if (ms < 10000)
            return " ";
        return "";
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

        /** Queries above this threshold → WARN box. Default: 1000ms. */
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
