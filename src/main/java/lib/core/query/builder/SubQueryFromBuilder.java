package lib.core.query.builder;

import lib.core.query.BaseQuery;

/**
 * Intermediate builder for setting subquery alias fluently.
 * <p>Usage: {@code .from(subQuery).as("alias")}
 * 
 * @param <T> the concrete query type
 */
public final class SubQueryFromBuilder<T extends BaseQuery<T>> {
    private final T query;

    public SubQueryFromBuilder(T query) {
        this.query = query;
    }

    /**
     * Set the alias for this subquery.
     *
     * @param alias the alias name
     * @return the parent query for further chaining
     */
    public T as(String alias) {
        query.fromSubQueryAlias = alias;
        return query;
    }
}
