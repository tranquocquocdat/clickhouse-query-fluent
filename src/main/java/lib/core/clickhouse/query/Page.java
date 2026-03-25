package lib.core.clickhouse.query;

import java.util.List;

/**
 * Paginated query result — holds data + total count from a single query execution.
 *
 * <pre>{@code
 * Page<Report> page = ClickHouseQuery.select("user_id", "amount")
 *     .from("orders")
 *     .where("tenant_id").eq(tenantId)
 *     .orderBy("created_at", SortOrder.DESC)
 *     .queryPage(0, 10, namedJdbc, rowMapper);
 *
 * page.getData();       // List<Report>
 * page.getTotal();      // 1234
 * page.getTotalPages(); // 124
 * }</pre>
 *
 * @param <T> the row type
 */
public final class Page<T> {

    private final List<T> data;
    private final long total;
    private final int page;
    private final int pageSize;

    public Page(List<T> data, long total, int page, int pageSize) {
        this.data = data;
        this.total = total;
        this.page = page;
        this.pageSize = pageSize;
    }

    /** The paginated data rows. */
    public List<T> getData() { return data; }

    /** Total number of rows matching the WHERE conditions (before LIMIT). */
    public long getTotal() { return total; }

    /** Current page index (0-based). */
    public int getPage() { return page; }

    /** Page size (rows per page). */
    public int getPageSize() { return pageSize; }

    /** Total number of pages. */
    public int getTotalPages() {
        return pageSize <= 0 ? 0 : (int) Math.ceil((double) total / pageSize);
    }

    /** Whether there is a next page. */
    public boolean hasNext() {
        return page < getTotalPages() - 1;
    }

    /** Whether there is a previous page. */
    public boolean hasPrevious() {
        return page > 0;
    }

    /** Whether the result is empty. */
    public boolean isEmpty() {
        return data == null || data.isEmpty();
    }
}
