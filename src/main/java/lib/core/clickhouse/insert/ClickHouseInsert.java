package lib.core.clickhouse.insert;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Function;

/**
 * Fluent INSERT builder for ClickHouse.
 *
 * <p>
 * Usage:
 * 
 * <pre>{@code
 * ClickHouseInsert.into("wallet_transaction")
 *         .columns("id", "user_id", "amount", "created_at")
 *         .executeBatch(namedJdbc, transactions, t -> CHParams.of()
 *                 .set("id", t.getId())
 *                 .set("userId", t.getUserId())
 *                 .set("amount", t.getAmount())
 *                 .setTimestamp("createdAt", t.getCreatedAt())
 *                 .build());
 * }</pre>
 */
public final class ClickHouseInsert {

    private String tableName;
    private final List<String> columnNames = new ArrayList<>();

    private ClickHouseInsert() {
    }

    // ── Factory ──────────────────────────────────────────────────────────

    /** Start an INSERT statement for the given table. */
    public static ClickHouseInsert into(String table) {
        ClickHouseInsert insert = new ClickHouseInsert();
        insert.tableName = table;
        return insert;
    }

    // ── Columns ──────────────────────────────────────────────────────────

    /** Define columns for the INSERT statement. */
    public ClickHouseInsert columns(String... columns) {
        this.columnNames.addAll(List.of(columns));
        return this;
    }

    // ── Build ────────────────────────────────────────────────────────────

    /**
     * Build the INSERT SQL.
     * Column names are used as-is for columns; camelCase versions are used as
     * parameter names.
     */
    public String toSql() {
        StringJoiner colJoiner = new StringJoiner(", ");
        StringJoiner valJoiner = new StringJoiner(", ");

        for (String col : columnNames) {
            colJoiner.add(col);
            valJoiner.add(":" + toCamelCase(col));
        }

        return "INSERT INTO " + tableName +
                " (" + colJoiner + ") VALUES (" + valJoiner + ")";
    }

    // ── Execute ──────────────────────────────────────────────────────────

    /** Execute a single insert. */
    public <T> void execute(NamedParameterJdbcTemplate jdbc, T entity,
            Function<T, MapSqlParameterSource> mapper) {
        jdbc.update(toSql(), mapper.apply(entity));
    }

    /** Execute batch insert. */
    public <T> void executeBatch(NamedParameterJdbcTemplate jdbc, List<T> entities,
            Function<T, MapSqlParameterSource> mapper) {
        String sql = toSql();
        SqlParameterSource[] batchParams = entities.stream()
                .map(mapper::apply)
                .toArray(SqlParameterSource[]::new);
        jdbc.batchUpdate(sql, batchParams);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /**
     * Convert snake_case column name to camelCase parameter name.
     * Example: "user_id" → "userId", "created_at" → "createdAt"
     */
    static String toCamelCase(String snakeCase) {
        StringBuilder result = new StringBuilder();
        boolean nextUpper = false;
        for (char c : snakeCase.toCharArray()) {
            if (c == '_') {
                nextUpper = true;
            } else if (nextUpper) {
                result.append(Character.toUpperCase(c));
                nextUpper = false;
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }
}
