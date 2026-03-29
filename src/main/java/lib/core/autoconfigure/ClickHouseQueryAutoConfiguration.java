package lib.core.autoconfigure;

import lib.core.query.observe.LoggingQueryObserver;
import lib.core.query.observe.QueryObserver;
import lib.core.query.observe.QueryObserverRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot AutoConfiguration for {@code clickhouse-query}.
 *
 * <p>
 * Activated when the library JAR is on the classpath <b>and</b>
 * {@code clickhouse-query.logging.enabled=true} is present in
 * {@code application.yml} (disabled by default — zero overhead if not
 * configured).
 *
 * <p>
 * Usage — add to {@code application.yml} in the consuming project:
 * 
 * <pre>
 * clickhouse-query:
 *   logging:
 *     enabled: true          # ← flip this switch to activate
 *     log-sql: true
 *     log-params: false      # caution: may expose PII
 *     slow-query-ms: 500
 *   metrics:
 *     enabled: true          # cache HIT / MISS at INFO level
 * </pre>
 *
 * <p>
 * No {@code @Configuration} class or manual registration needed.
 * The auto-config creates a {@link QueryObserver} bean and registers it
 * with {@link QueryObserverRegistry} during application startup.
 * Override by defining your own {@link QueryObserver} {@code @Bean}.
 */
@AutoConfiguration
@EnableConfigurationProperties(ClickHouseQueryProperties.class)
public class ClickHouseQueryAutoConfiguration {

    /**
     * Creates and registers a {@link LoggingQueryObserver} from YAML properties.
     *
     * <p>
     * Skipped when:
     * <ul>
     * <li>{@code clickhouse-query.logging.enabled} is {@code false} or absent</li>
     * <li>A custom {@link QueryObserver} bean already exists in the context</li>
     * </ul>
     */
    @Bean
    @ConditionalOnMissingBean(QueryObserver.class)
    @ConditionalOnProperty(prefix = "clickhouse-query.logging", name = "enabled", havingValue = "true", matchIfMissing = false)
    public QueryObserver clickHouseQueryObserver(ClickHouseQueryProperties props) {
        ClickHouseQueryProperties.Logging logging = props.getLogging();
        ClickHouseQueryProperties.Metrics metrics = props.getMetrics();

        LoggingQueryObserver observer = LoggingQueryObserver.builder()
                .logSql(logging.isLogSql())
                .logParams(logging.isLogParams())
                .slowQueryThresholdMs(logging.getSlowQueryMs())
                .trackMetrics(metrics.isEnabled())
                .build();

        QueryObserverRegistry.register(observer);
        return observer;
    }
}
