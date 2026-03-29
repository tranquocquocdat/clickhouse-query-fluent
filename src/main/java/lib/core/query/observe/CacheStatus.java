package lib.core.query.observe;

/**
 * Indicates whether the result came from cache or was fetched from the
 * database.
 */
public enum CacheStatus {
    /** Cache was not configured for this query. */
    DISABLED,
    /** Result was found in cache — no database call was made. */
    HIT,
    /** Result was not found in cache — database was queried. */
    MISS
}
