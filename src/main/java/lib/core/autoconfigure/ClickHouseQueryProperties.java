package lib.core.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for {@code clickhouse-query} auto-configuration.
 *
 * <p>
 * Configure in {@code application.yml}:
 * 
 * <pre>
 * clickhouse-query:
 *   logging:
 *     enabled: true
 *     log-sql: true
 *     log-params: false
 *     slow-query-ms: 1000
 *   metrics:
 *     enabled: true
 * </pre>
 */
@ConfigurationProperties(prefix = "clickhouse-query")
public class ClickHouseQueryProperties {

    private final Logging logging = new Logging();
    private final Metrics metrics = new Metrics();

    public Logging getLogging() {
        return logging;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    // ── Logging ──────────────────────────────────────────────────────────

    public static class Logging {
        /** Enable the built-in QueryObserver bean. */
        private boolean enabled = false;

        /** Log every SQL statement at DEBUG level. Default: true. */
        private boolean logSql = true;

        /**
         * Also log bind parameter values alongside SQL.
         * <b>Caution: may expose PII.</b> Default: false.
         */
        private boolean logParams = false;

        /**
         * Queries taking longer than this threshold (ms) are logged at WARN.
         * Default: 1000 ms.
         */
        private long slowQueryMs = 1_000L;

        public boolean isEnabled() {
            return enabled;
        }

        public boolean isLogSql() {
            return logSql;
        }

        public boolean isLogParams() {
            return logParams;
        }

        public long getSlowQueryMs() {
            return slowQueryMs;
        }

        public void setEnabled(boolean v) {
            this.enabled = v;
        }

        public void setLogSql(boolean v) {
            this.logSql = v;
        }

        public void setLogParams(boolean v) {
            this.logParams = v;
        }

        public void setSlowQueryMs(long v) {
            this.slowQueryMs = v;
        }
    }

    // ── Metrics ───────────────────────────────────────────────────────────

    public static class Metrics {
        /** Log cache HIT / MISS at INFO level. Default: true. */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean v) {
            this.enabled = v;
        }
    }
}
