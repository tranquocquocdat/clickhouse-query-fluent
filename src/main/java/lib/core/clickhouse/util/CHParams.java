package lib.core.clickhouse.util;

import lib.core.query.util.SqlParams;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.time.Instant;
import java.util.List;

/**
 * Fluent builder for ClickHouse insert parameters.
 *
 * @deprecated Use {@link SqlParams} instead (database-agnostic).
 */
@Deprecated
public final class CHParams {

    private final SqlParams delegate = SqlParams.of();

    private CHParams() {
    }

    /** Create a new fluent param builder. */
    public static CHParams of() {
        return new CHParams();
    }

    // ── Basic ────────────────────────────────────────────────────────────

    /** @deprecated Use {@link SqlParams#set(String, Object)} */
    @Deprecated
    public CHParams set(String name, Object value) {
        delegate.set(name, value);
        return this;
    }

    /** @deprecated Use {@link SqlParams#setOrDefault(String, Object, Object)} */
    @Deprecated
    public <T> CHParams setOrDefault(String name, T value, T defaultValue) {
        delegate.setOrDefault(name, value, defaultValue);
        return this;
    }

    /** @deprecated Use {@link SqlParams#setIfNotNull(String, Object)} */
    @Deprecated
    public CHParams setIfNotNull(String name, Object value) {
        delegate.setIfNotNull(name, value);
        return this;
    }

    // ── Enum ─────────────────────────────────────────────────────────────

    /** @deprecated Use {@link SqlParams#setEnum(String, Enum)} */
    @Deprecated
    public CHParams setEnum(String name, Enum<?> value) {
        delegate.setEnum(name, value);
        return this;
    }

    // ── Timestamp ────────────────────────────────────────────────────────

    /** @deprecated Use {@link SqlParams#setTimestamp(String, Instant)} */
    @Deprecated
    public CHParams setTimestamp(String name, Instant value) {
        delegate.setTimestamp(name, value);
        return this;
    }

    // ── Array ────────────────────────────────────────────────────────────

    /** @deprecated Use {@link SqlParams#setArray(String, List, Class)} */
    @Deprecated
    public <T> CHParams setArray(String name, List<T> list, Class<T> type) {
        delegate.setArray(name, list, type);
        return this;
    }

    // ── Build ────────────────────────────────────────────────────────────

    /** Return the built MapSqlParameterSource. */
    public MapSqlParameterSource build() {
        return delegate.build();
    }
}
