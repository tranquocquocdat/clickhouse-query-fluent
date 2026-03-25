package lib.core.clickhouse;

import java.util.StringJoiner;

/**
 * Fluent builder for LIKE/ILIKE search across multiple columns.
 * Created via {@link ClickHouseQuery#whereILike(String)} or {@link ClickHouseQuery#whereLike(String)}.
 *
 * <pre>{@code
 * .whereILike(keyword).on("session_id", "user_id")
 * .whereLike(keyword).on("session_id", "user_id")
 * }</pre>
 */
public final class WhereILikeBuilder {
    private final ClickHouseQuery query;
    private final String keyword;
    private final boolean caseSensitive;

    WhereILikeBuilder(ClickHouseQuery query, String keyword, boolean caseSensitive) {
        this.query = query;
        this.keyword = keyword;
        this.caseSensitive = caseSensitive;
    }

    /**
     * Apply LIKE/ILIKE search across the given columns (combined with OR).
     * Skipped when keyword is null or blank.
     *
     * @param columns the columns to search on
     * @return the parent query builder
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
}
