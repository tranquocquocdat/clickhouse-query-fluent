package lib.core.query.cursor;

import java.util.Objects;

/**
 * Request object for cursor-based (keyset) pagination.
 *
 * <p>
 * Create the first page with {@link #firstPage(int)}, then use the
 * {@code nextCursor} from {@link CursorPage} to build the next request:
 *
 * <pre>{@code
 * // First page
 * CursorPage<Report> page = query.queryCursor(
 *         CursorRequest.firstPage(50), jdbc, Report.class, keyExtractor);
 *
 * // Next page
 * if (page.hasNext()) {
 *     CursorPage<Report> page2 = query.queryCursor(
 *             CursorRequest.nextPage(50, page.getNextCursor()), jdbc, Report.class, keyExtractor);
 * }
 * }</pre>
 */
public final class CursorRequest {

    private final int limit;
    private final String cursor; // null = first page

    private CursorRequest(int limit, String cursor) {
        if (limit <= 0)
            throw new IllegalArgumentException("limit must be > 0, got: " + limit);
        this.limit = limit;
        this.cursor = cursor;
    }

    /**
     * Start from the very beginning (no cursor).
     *
     * @param limit number of rows per page
     */
    public static CursorRequest firstPage(int limit) {
        return new CursorRequest(limit, null);
    }

    /**
     * Continue from a cursor token returned by {@link CursorPage#getNextCursor()}.
     *
     * @param limit  number of rows per page
     * @param cursor opaque cursor token from the previous page
     */
    public static CursorRequest nextPage(int limit, String cursor) {
        return new CursorRequest(limit, Objects.requireNonNull(cursor, "cursor must not be null for nextPage"));
    }

    /** Number of rows per page. */
    public int getLimit() {
        return limit;
    }

    /**
     * Opaque cursor token, {@code null} for the first page.
     * Decoded internally by the library — do not parse.
     */
    public String getCursor() {
        return cursor;
    }

    /** Whether this is the first page (no cursor). */
    public boolean isFirstPage() {
        return cursor == null;
    }
}
