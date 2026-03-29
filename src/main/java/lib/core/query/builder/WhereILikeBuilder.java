package lib.core.query.builder;

import lib.core.query.BaseQuery;

import java.util.StringJoiner;

/**
 * Fluent builder for LIKE/ILIKE search across multiple columns.
 * Created via {@link BaseQuery#whereILike(String)} or {@link BaseQuery#whereLike(String)}.
 */
public final class WhereILikeBuilder<T extends BaseQuery<T>> {
    private final T query;
    private final String keyword;
    private final boolean caseSensitive;

    public WhereILikeBuilder(T query, String keyword, boolean caseSensitive) {
        this.query = query;
        this.keyword = keyword;
        this.caseSensitive = caseSensitive;
    }

    /**
     * Apply LIKE/ILIKE search across the given columns (combined with OR).
     * Uses {@code %keyword%} pattern. Skipped when keyword is null or blank.
     */
    public T on(String... columns) {
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

    /** Overload accepting Expr/Object columns directly (no .toString() needed). */
    public T on(Object... columns) {
        if (keyword == null || keyword.isBlank()) return query;
        String operator = caseSensitive ? "LIKE" : "ILIKE";
        String paramName = caseSensitive ? "_likeKeyword" : "_keyword";
        StringJoiner or = new StringJoiner(" OR ", "(", ")");
        for (Object col : columns) {
            or.add(col.toString() + " " + operator + " :" + paramName);
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
    public T onPrefix(String... columns) {
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

    /** Overload accepting Expr/Object columns directly (no .toString() needed). */
    public T onPrefix(Object... columns) {
        if (keyword == null || keyword.isBlank()) return query;
        String operator = caseSensitive ? "LIKE" : "ILIKE";
        String paramName = caseSensitive ? "_likePrefixKeyword" : "_prefixKeyword";
        StringJoiner or = new StringJoiner(" OR ", "(", ")");
        for (Object col : columns) {
            or.add(col.toString() + " " + operator + " :" + paramName);
        }
        query.whereClauses.add(or.toString());
        query.params.addValue(paramName, keyword.trim() + "%");
        return query;
    }
}
