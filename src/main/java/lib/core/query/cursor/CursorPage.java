package lib.core.query.cursor;

import java.util.Collections;
import java.util.List;

/**
 * Response for cursor-based (keyset) pagination.
 *
 * <p>
 * Contains the current page's data and a {@code nextCursor} token that
 * can be passed to {@link CursorRequest#nextPage(int, String)} to fetch the
 * following page. If {@link #hasNext()} is {@code false}, this is the last
 * page.
 *
 * <pre>{@code
 * CursorPage<Report> page = query.queryCursor(request, jdbc, Report.class, extractor);
 *
 * page.getData(); // List<Report> — current page rows
 * page.hasNext(); // boolean
 * page.getNextCursor(); // String — pass to CursorRequest.nextPage() — null on last page
 * page.getCount(); // int — rows on this page
 * }</pre>
 *
 * @param <T> the row type
 */
public final class CursorPage<T> {

    private final List<T> data;
    private final String nextCursor; // null = no more pages
    private final boolean hasNext;

    public CursorPage(List<T> data, String nextCursor) {
        this.data = Collections.unmodifiableList(data);
        this.nextCursor = nextCursor;
        this.hasNext = nextCursor != null;
    }

    /** Rows on this page. */
    public List<T> getData() {
        return data;
    }

    /**
     * Opaque cursor token to fetch the next page.
     * {@code null} when this is the last page.
     */
    public String getNextCursor() {
        return nextCursor;
    }

    /** {@code true} if there is at least one more page after this one. */
    public boolean hasNext() {
        return hasNext;
    }

    /** Number of rows on this page. */
    public int getCount() {
        return data.size();
    }

    /** Whether this page has no rows. */
    public boolean isEmpty() {
        return data.isEmpty();
    }
}
