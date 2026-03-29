package lib.core.query.cache;

/**
 * Configuration for caching a specific query.
 */
public class CacheOptions {

    private final QueryCacheManager manager;
    private final long ttlSeconds;

    public CacheOptions(QueryCacheManager manager, long ttlSeconds) {
        this.manager = manager;
        this.ttlSeconds = ttlSeconds;
    }

    public QueryCacheManager getManager() {
        return manager;
    }

    public long getTtlSeconds() {
        return ttlSeconds;
    }
}
