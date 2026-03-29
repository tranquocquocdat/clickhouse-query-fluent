package lib.core.query.observe;

/**
 * Listener interface for query lifecycle events.
 *
 * <p>
 * Implement this interface and register via
 * {@link QueryObserverRegistry#register(QueryObserver)}
 * to receive structured events after every query execution.
 *
 * <p>
 * Typical usage in a Spring Boot application:
 * 
 * <pre>
 * {@code
 * &#64;Bean
 * public QueryObserver queryObserver() {
 *     return LoggingQueryObserver.builder()
 *         .logSql(true)
 *         .logParams(true)
 *         .slowQueryThresholdMs(1000)
 *         .build();
 * }
 *
 * @PostConstruct
 * public void setup() {
 *     QueryObserverRegistry.register(queryObserver());
 * }
 * }
 * </pre>
 */
public interface QueryObserver {

    /**
     * Called after every query execution (list, one, page, stream).
     *
     * @param event the query event containing SQL, params, duration, cache status,
     *              etc.
     */
    void onQuery(QueryEvent event);
}
