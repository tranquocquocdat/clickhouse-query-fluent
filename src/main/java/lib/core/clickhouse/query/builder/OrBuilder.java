package lib.core.clickhouse.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for OR-grouped WHERE conditions.
 * Created via {@link ClickHouseQuery#whereOr(java.util.function.Consumer)}.
 */
public final class OrBuilder {
    private final ClickHouseQuery query;
    private final List<String> conditions = new ArrayList<>();
    private static int orSeq = 0;

    public OrBuilder(ClickHouseQuery query) {
        this.query = query;
    }

    /** {@code column = value} */
    public OrBuilder add(String column, Object value) {
        String p = "_or" + (orSeq++);
        conditions.add(column + " = :" + p);
        query.params.addValue(p, value);
        return this;
    }

    /** Raw condition with param. */
    public OrBuilder addRaw(String condition, String paramName, Object value) {
        conditions.add(condition);
        query.params.addValue(paramName, value);
        return this;
    }

    /** Raw condition without param. */
    public OrBuilder addRaw(String condition) {
        conditions.add(condition);
        return this;
    }

    public void apply() {
        if (!conditions.isEmpty()) {
            query.whereClauses.add("(" + String.join(" OR ", conditions) + ")");
        }
    }
}
