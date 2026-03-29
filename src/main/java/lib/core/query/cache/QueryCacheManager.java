package lib.core.query.cache;

/**
 * Interface for integrating an external cache (e.g. Redis, Caffeine)
 * with the ClickHouse query builder.
 */
public interface QueryCacheManager {

    /**
     * Retrieve data from the cache.
     *
     * @param key        the auto-generated cache key (hash of SQL + params)
     * @param returnType the class of the expected result (for JSON deserialization)
     * @param isList     whether the expected result is a List or a single Object
     * @return the cached object/list, or null if cache miss or error
     */
    <T> Object get(String key, Class<T> returnType, boolean isList);

    /**
     * Store data in the cache.
     *
     * @param key        the auto-generated cache key
     * @param data       the result (List, Page, or single Object) to cache
     * @param ttlSeconds time-to-live in seconds
     */
    void put(String key, Object data, long ttlSeconds);
}
