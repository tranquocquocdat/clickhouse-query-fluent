package lib.core.query.observe;

import java.util.Map;

/**
 * Immutable event emitted after every query execution.
 * Passed to {@link QueryObserver} hooks.
 */
public final class QueryEvent {

    /** Type of execution method used. */
    public enum QueryType {
        LIST, ONE, PAGE, STREAM
    }

    private final String sql;
    private final Map<String, Object> params; // null when logParams = false
    private final long durationMs;
    private final QueryType queryType;
    private final CacheStatus cacheStatus;
    private final int resultCount; // -1 for stream
    private final int page; // only for PAGE, else -1
    private final int pageSize; // only for PAGE, else -1

    public QueryEvent(
            String sql, Map<String, Object> params,
            long durationMs, QueryType queryType,
            CacheStatus cacheStatus, int resultCount,
            int page, int pageSize) {
        this.sql = sql;
        this.params = params;
        this.durationMs = durationMs;
        this.queryType = queryType;
        this.cacheStatus = cacheStatus;
        this.resultCount = resultCount;
        this.page = page;
        this.pageSize = pageSize;
    }

    public String getSql() {
        return sql;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public CacheStatus getCacheStatus() {
        return cacheStatus;
    }

    public int getResultCount() {
        return resultCount;
    }

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }

    @Override
    public String toString() {
        return String.format("[%s] %s | cache=%s | rows=%d | %dms",
                queryType, sql, cacheStatus, resultCount, durationMs);
    }
}
