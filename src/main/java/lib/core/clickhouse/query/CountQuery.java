package lib.core.clickhouse.query;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Holder for a {@code SELECT COUNT(*) FROM (subquery)} pattern.
 * Created via {@link ClickHouseQuery#count(ClickHouseQuery)}.
 *
 * <pre>{@code
 * long total = ClickHouseQuery
 *     .count(
 *         ClickHouseQuery.select("user_id", "session_id")
 *             .from("order_items")
 *             .where("created_at").between(from, to)
 *             .groupBy("user_id", "session_id")
 *     )
 *     .execute(namedJdbc);
 * }</pre>
 */
public final class CountQuery {
    private final ClickHouseQuery subQuery;

    CountQuery(ClickHouseQuery subQuery) {
        this.subQuery = subQuery;
    }

    /**
     * Execute the count query.
     *
     * @param jdbc the JDBC template
     * @return the total count
     */
    public long execute(NamedParameterJdbcTemplate jdbc) {
        String sql = "SELECT count(*) FROM (" + subQuery.toSql() + ")";
        Long result = jdbc.queryForObject(sql, subQuery.params, Long.class);
        return result != null ? result : 0;
    }

    /** Get the generated SQL (useful for debugging/testing). */
    public String toSql() {
        return "SELECT count(*) FROM (" + subQuery.toSql() + ")";
    }
}
