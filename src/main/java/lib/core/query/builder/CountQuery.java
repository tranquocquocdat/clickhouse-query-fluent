package lib.core.query.builder;

import lib.core.query.BaseQuery;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Holder for a {@code SELECT COUNT(*) FROM (subquery)} pattern.
 * Created via static factory methods in query classes.
 */
public final class CountQuery<T extends BaseQuery<T>> {
    private final T subQuery;

    public CountQuery(T subQuery) {
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
