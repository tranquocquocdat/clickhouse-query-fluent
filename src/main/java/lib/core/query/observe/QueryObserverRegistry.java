package lib.core.query.observe;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Application-scoped static registry for the active {@link QueryObserver}.
 *
 * <p>
 * Only one observer can be registered at a time.
 * Register your observer once at application startup (e.g.,
 * {@code @PostConstruct}).
 *
 * <pre>
 * {@code
 * QueryObserverRegistry.register(
 *         LoggingQueryObserver.builder().logSql(true).logParams(false).build());
 * }
 * </pre>
 */
public final class QueryObserverRegistry {

    private static final AtomicReference<QueryObserver> INSTANCE = new AtomicReference<>(null);

    private QueryObserverRegistry() {
    }

    /**
     * Register a {@link QueryObserver}. Replaces any previously registered
     * observer.
     *
     * @param observer the observer to activate; {@code null} disables observation.
     */
    public static void register(QueryObserver observer) {
        INSTANCE.set(observer);
    }

    /**
     * Returns the currently registered observer, or {@code null} if none is
     * registered.
     */
    public static QueryObserver get() {
        return INSTANCE.get();
    }

    /**
     * Emits a {@link QueryEvent} to the registered observer if one is active.
     * Safe to call without checking whether an observer is registered.
     */
    public static void emit(QueryEvent event) {
        QueryObserver obs = INSTANCE.get();
        if (obs != null) {
            try {
                obs.onQuery(event);
            } catch (Exception e) {
                // Never let observer errors propagate into query path
            }
        }
    }
}
