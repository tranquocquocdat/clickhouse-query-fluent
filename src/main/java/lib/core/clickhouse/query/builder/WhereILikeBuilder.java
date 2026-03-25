package lib.core.clickhouse.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;

import java.util.StringJoiner;

/**
 * Fluent builder for LIKE/ILIKE search across multiple columns.
 * Created via {@link ClickHouseQuery#whereILike(String)} or {@link ClickHouseQuery#whereLike(String)}.
 */
public final class WhereILikeBuilder {
    private final ClickHouseQuery query;
    private final String keyword;
    private final boolean caseSensitive;

    public WhereILikeBuilder(ClickHouseQuery query, String keyword, boolean caseSensitive) {
        this.query = query;
        this.keyword = keyword;
        this.caseSensitive = caseSensitive;
    }

    /**
     * Apply LIKE/ILIKE search across the given columns (combined with OR).
     * Uses {@code %keyword%} pattern. Skipped when keyword is null or blank.
     */
    public ClickHouseQuery on(String... columns) {
        if (keyword == null || keyword.isBlank()) return query;
        String operator = caseSensitive ? "LIKE" : "ILIKE";
        String paramName = caseSensitive ? "_likeKeyword" : "_keyword";
        StringJoiner or = new StringJoiner(" OR ", "(", ")");
        for (String col : columns) {
            or.add(col + " " + operator + " :" + paramName);
        }
        query.whereClauses.add(or.toString());
        query.params.addValue(paramName, "%" + keyword.trim() + "%");
        return query;
    }

    /**
     * Apply prefix-only LIKE/ILIKE search across the given columns (combined with OR).
     * Uses {@code keyword%} pattern — optimized for index prefix matching.
     * Skipped when keyword is null or blank.
     */
    public ClickHouseQuery onPrefix(String... columns) {
        if (keyword == null || keyword.isBlank()) return query;
        String operator = caseSensitive ? "LIKE" : "ILIKE";
        String paramName = caseSensitive ? "_likePrefixKeyword" : "_prefixKeyword";
        StringJoiner or = new StringJoiner(" OR ", "(", ")");
        for (String col : columns) {
            or.add(col + " " + operator + " :" + paramName);
        }
        query.whereClauses.add(or.toString());
        query.params.addValue(paramName, keyword.trim() + "%");
        return query;
    }
}
