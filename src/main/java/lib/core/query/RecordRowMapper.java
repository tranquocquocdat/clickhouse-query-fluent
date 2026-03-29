package lib.core.query;

import lib.core.clickhouse.util.CHStringUtils;
import lib.core.query.RecordMapperCache.RecordMeta;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link RowMapper} implementation for Java {@code record} types.
 * <p>
 * Automatically maps {@code snake_case} column names from the ResultSet
 * to the record's component names (camelCase).
 *
 * <pre>{@code
 * // Given this record:
 * public record OrderHeader(String userId, String orderId, String tenantId) {
 * }
 *
 * // And a query returning columns: user_id, order_id, tenant_id
 * List<OrderHeader> list = query.query(namedJdbc, OrderHeader.class);
 * }</pre>
 *
 * <h3>Two-tier caching</h3>
 * <ul>
 * <li><b>App-scoped</b> — {@link RecordMapperCache} holds one
 * {@link RecordMeta} per record
 * class (constructor + components + componentIndex). Resolved once, reused
 * forever.</li>
 * <li><b>Query-scoped</b> — {@link #rsMapping} caches the ResultSet column →
 * component-index
 * mapping for the current query. Built on the first row, reused for every
 * subsequent
 * row of the same result set.</li>
 * </ul>
 *
 * @param <T> the record type
 */
public final class RecordRowMapper<T extends Record> implements RowMapper<T> {

    /**
     * App-scoped metadata: constructor + components + componentIndex.
     * Fetched from {@link RecordMapperCache} — never rebuilt after first access.
     */
    private final RecordMeta<T> meta;

    /**
     * Query-scoped cache: component name → ResultSet column index (1-based).
     * {@code null} until the first row; built once from
     * {@link ResultSet#getMetaData()}
     * and reused for every subsequent row of the same result set.
     * <p>
     * Not thread-safe by design — Spring JDBC calls {@code mapRow()} sequentially
     * in one thread per query, so no synchronization is needed.
     */
    private Map<String, Integer> rsMapping;

    private RecordRowMapper(Class<T> recordType) {
        this.meta = RecordMapperCache.get(recordType);
    }

    /**
     * Create a {@code RecordRowMapper} for the given record class.
     *
     * @param recordType the Java record class
     * @param <T>        the record type
     * @return a new per-query mapper (backed by the app-scoped metadata cache)
     */
    public static <T extends Record> RecordRowMapper<T> of(Class<T> recordType) {
        return new RecordRowMapper<>(recordType);
    }

    @Override
    public T mapRow(ResultSet rs, int rowNum) throws SQLException {
        // Build RS mapping once (first row), reuse for all subsequent rows
        if (rsMapping == null) {
            rsMapping = buildRsMapping(rs);
        }

        Object[] args = new Object[meta.components.length];
        for (int i = 0; i < meta.components.length; i++) {
            String name = meta.components[i].getName();
            Integer rsColIndex = rsMapping.get(name);
            if (rsColIndex != null) {
                args[i] = getValue(rs, rsColIndex, meta.components[i].getType());
            }
            // stays null / primitive-default if column not in result set
        }

        try {
            return meta.constructor.newInstance(args);
        } catch (Exception e) {
            throw new SQLException("Failed to instantiate record: " + e.getMessage(), e);
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /**
     * Build the query-scoped mapping: component name → ResultSet column index
     * (1-based).
     * <p>
     * Converts ResultSet column labels from {@code snake_case} to
     * {@code camelCase},
     * then matches against {@link RecordMeta#componentIndex}.
     * Only columns that have a matching record component are stored.
     * Called exactly once per query (on the first row).
     */
    private Map<String, Integer> buildRsMapping(ResultSet rs) throws SQLException {
        var meta = rs.getMetaData();
        Map<String, Integer> map = new HashMap<>(meta.getColumnCount() * 2);
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String camelCase = toCamelCase(meta.getColumnLabel(i));
            if (this.meta.componentIndex.containsKey(camelCase)) {
                map.put(camelCase, i);
            }
        }
        return map;
    }

    /**
     * Extract a typed value from the ResultSet based on the target field type.
     */
    private Object getValue(ResultSet rs, int colIndex, Class<?> type) throws SQLException {
        if (rs.getObject(colIndex) == null) {
            return getDefault(type);
        }
        if (type == String.class)
            return rs.getString(colIndex);
        if (type == int.class || type == Integer.class)
            return rs.getInt(colIndex);
        if (type == long.class || type == Long.class)
            return rs.getLong(colIndex);
        if (type == double.class || type == Double.class)
            return rs.getDouble(colIndex);
        if (type == float.class || type == Float.class)
            return rs.getFloat(colIndex);
        if (type == boolean.class || type == Boolean.class)
            return rs.getBoolean(colIndex);
        if (type == BigDecimal.class)
            return rs.getBigDecimal(colIndex);
        if (type == Instant.class) {
            Timestamp ts = rs.getTimestamp(colIndex);
            return ts != null ? ts.toInstant() : null;
        }
        if (type == Timestamp.class)
            return rs.getTimestamp(colIndex);
        return rs.getObject(colIndex, type);
    }

    /**
     * Default values for primitive types — prevents record instantiation failures
     * when a column is absent from the result set.
     */
    private Object getDefault(Class<?> type) {
        if (type == int.class)
            return 0;
        if (type == long.class)
            return 0L;
        if (type == double.class)
            return 0.0;
        if (type == float.class)
            return 0.0f;
        if (type == boolean.class)
            return false;
        return null;
    }

    /**
     * Convert {@code snake_case} column label to {@code camelCase} component name.
     */
    private static String toCamelCase(String snake) {
        return CHStringUtils.toCamelCase(snake);
    }
}
