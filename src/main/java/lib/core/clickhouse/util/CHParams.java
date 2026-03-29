package lib.core.clickhouse.util;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Fluent builder for ClickHouse insert parameters.
 *
 * <p>
 * Usage:
 * 
 * <pre>{@code
 * MapSqlParameterSource params = CHParams.of()
 *         .set("userId", user.getId())
 *         .setOrDefault("amount", user.getAmount(), BigDecimal.ZERO)
 *         .setEnum("status", user.getStatus())
 *         .setTimestamp("createdAt", user.getCreatedAt())
 *         .setArray("tags", user.getTags(), String.class)
 *         .build();
 * }</pre>
 */
public final class CHParams {

    private final MapSqlParameterSource params = new MapSqlParameterSource();

    private CHParams() {
    }

    /** Create a new fluent param builder. */
    public static CHParams of() {
        return new CHParams();
    }

    // ── Basic ────────────────────────────────────────────────────────────

    /** Set a parameter value (can be null). */
    public CHParams set(String name, Object value) {
        params.addValue(name, value);
        return this;
    }

    /** Set a parameter, using defaultValue when value is null. */
    public <T> CHParams setOrDefault(String name, T value, T defaultValue) {
        params.addValue(name, value != null ? value : defaultValue);
        return this;
    }

    /**
     * Set a parameter only when value is non-null.
     * If value is {@code null}, the parameter is NOT added to the source at all.
     *
     * <p>
     * <b>⚠ WARNING — INSERT with fixed columns:</b> If you use this in an INSERT
     * built with
     * {@link lib.core.clickhouse.insert.ClickHouseInsert} and the column is listed
     * in
     * {@code .columns(...)}, a missing parameter will cause Spring to throw
     * {@code "No value supplied for the SQL parameter 'xxx'"}.
     *
     * <p>
     * Safe use cases:
     * <ul>
     * <li>Dynamic UPDATE: {@code SET col = :col} only added when non-null</li>
     * <li>INSERT with dynamic column list (columns added conditionally)</li>
     * </ul>
     *
     * <pre>{@code
     * // ✅ Safe — dynamic UPDATE
     * CHParams.of()
     *     .set("id", entity.getId())
     *     .setIfNotNull("note", entity.getNote())  // skip col entirely if null
     *     .build();
     *
     * // ❌ Unsafe — fixed column INSERT where note column is always listed
     * ClickHouseInsert.into("t").columns("id", "note")
     *     .execute(jdbc, entity, e -> CHParams.of()
     *         .set("id", e.getId())
     *         .setIfNotNull("note", e.getNote())  // throws if note is null!
     *         .build());
     *
     * // ✅ Use setOrDefault instead for INSERT
     * .setOrDefault("note", entity.getNote(), "")
     * }</pre>
     */
    public CHParams setIfNotNull(String name, Object value) {
        if (value != null) {
            params.addValue(name, value);
        }
        return this;
    }

    // ── Enum ─────────────────────────────────────────────────────────────

    /** Set an enum as its name(), or null if enum is null. */
    public CHParams setEnum(String name, Enum<?> value) {
        params.addValue(name, value != null ? value.name() : null);
        return this;
    }

    // ── Timestamp ────────────────────────────────────────────────────────

    /** Set an Instant as java.sql.Timestamp. */
    public CHParams setTimestamp(String name, Instant value) {
        params.addValue(name, value != null ? java.sql.Timestamp.from(value) : null);
        return this;
    }

    // ── Array ────────────────────────────────────────────────────────────

    /** Set a List as an array, empty array when list is null. */
    @SuppressWarnings("unchecked")
    public <T> CHParams setArray(String name, List<T> list, Class<T> type) {
        if (list != null) {
            params.addValue(name, list.toArray((T[]) java.lang.reflect.Array.newInstance(type, 0)));
        } else {
            params.addValue(name, java.lang.reflect.Array.newInstance(type, 0));
        }
        return this;
    }

    // ── Build ────────────────────────────────────────────────────────────

    /** Return the built MapSqlParameterSource. */
    public MapSqlParameterSource build() {
        return params;
    }
}
