package lib.core.clickhouse.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Holder for a {@code SELECT COUNT(*) FROM (subquery)} pattern.
 * Created via {@link ClickHouseQuery#count(ClickHouseQuery)}.
 */
public final class CountQuery {
    private final ClickHouseQuery subQuery;

    public CountQuery(ClickHouseQuery subQuery) {
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
