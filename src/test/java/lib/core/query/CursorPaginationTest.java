package lib.core.query;

import lib.core.clickhouse.query.ClickHouseQuery;
import lib.core.query.cursor.CursorField;
import lib.core.query.cursor.CursorPage;
import lib.core.query.cursor.CursorRequest;
import lib.core.query.cursor.CursorToken;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for Cursor (Keyset) Pagination.
 */
@DisplayName("Cursor Pagination")
class CursorPaginationTest {

    // ── CursorToken round-trip ───────────────────────────────────────────

    @Nested
    @DisplayName("CursorToken")
    class CursorTokenTests {

        @Test
        @DisplayName("Encode → Decode round-trip: single field")
        void roundTrip_singleField() {
            List<CursorField> fields = List.of(CursorField.of("event_date", "2024-01-10"));
            String token = CursorToken.encode(fields);

            assertNotNull(token);
            List<CursorField> decoded = CursorToken.decode(token);
            assertEquals(1, decoded.size());
            assertEquals("event_date", decoded.get(0).getColumn());
            assertEquals("2024-01-10", decoded.get(0).getValue());
        }

        @Test
        @DisplayName("Encode → Decode round-trip: two fields")
        void roundTrip_twoFields() {
            List<CursorField> fields = List.of(
                    CursorField.of("event_date", "2024-01-10"),
                    CursorField.of("id", "9999"));
            String token = CursorToken.encode(fields);
            List<CursorField> decoded = CursorToken.decode(token);

            assertEquals(2, decoded.size());
            assertEquals("event_date", decoded.get(0).getColumn());
            assertEquals("2024-01-10", decoded.get(0).getValue());
            assertEquals("id", decoded.get(1).getColumn());
            assertEquals("9999", decoded.get(1).getValue());
        }

        @Test
        @DisplayName("encode() throws on null/empty fields")
        void encode_throwsOnEmpty() {
            assertThrows(IllegalArgumentException.class, () -> CursorToken.encode(List.of()));
            assertThrows(IllegalArgumentException.class, () -> CursorToken.encode(null));
        }

        @Test
        @DisplayName("decode() throws on blank token")
        void decode_throwsOnBlank() {
            assertThrows(IllegalArgumentException.class, () -> CursorToken.decode(""));
            assertThrows(IllegalArgumentException.class, () -> CursorToken.decode(null));
        }

        @Test
        @DisplayName("Token is URL-safe Base64 (no +, /, = characters)")
        void token_isUrlSafe() {
            String token = CursorToken.encode(List.of(CursorField.of("id", "12345")));
            assertFalse(token.contains("+"), "must not contain +");
            assertFalse(token.contains("/"), "must not contain /");
            assertFalse(token.contains("="), "must not contain padding =");
        }
    }

    // ── CursorRequest ───────────────────────────────────────────────────

    @Nested
    @DisplayName("CursorRequest")
    class CursorRequestTests {

        @Test
        @DisplayName("firstPage() has null cursor and isFirstPage=true")
        void firstPage_cursorIsNull() {
            CursorRequest req = CursorRequest.firstPage(50);
            assertEquals(50, req.getLimit());
            assertNull(req.getCursor());
            assertTrue(req.isFirstPage());
        }

        @Test
        @DisplayName("nextPage() has cursor and isFirstPage=false")
        void nextPage_hasCorrectCursor() {
            CursorRequest req = CursorRequest.nextPage(20, "someToken");
            assertEquals(20, req.getLimit());
            assertEquals("someToken", req.getCursor());
            assertFalse(req.isFirstPage());
        }

        @Test
        @DisplayName("limit <= 0 throws IllegalArgumentException")
        void invalidLimit_throws() {
            assertThrows(IllegalArgumentException.class, () -> CursorRequest.firstPage(0));
            assertThrows(IllegalArgumentException.class, () -> CursorRequest.firstPage(-1));
        }

        @Test
        @DisplayName("nextPage() with null cursor throws")
        void nextPage_nullCursor_throws() {
            assertThrows(NullPointerException.class, () -> CursorRequest.nextPage(10, null));
        }
    }

    // ── SQL generation via queryCursor (SQL tests) ──────────────────────

    @Nested
    @DisplayName("SQL Generation")
    @SuppressWarnings("unchecked")
    class SqlGenerationTests {

        @Test
        @DisplayName("First page: no cursor WHERE clause added")
        void firstPage_noWhereAdded() {
            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            when(jdbc.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                    .thenReturn(List.of("a", "b"));

            ClickHouseQuery.select("event_date", "id")
                    .from("events")
                    .where("tenant_id").eq("T001")
                    .orderBy("event_date", SortOrder.DESC)
                    .queryCursor(CursorRequest.firstPage(10), jdbc, (RowMapper<String>) (rs, n) -> "row",
                            row -> List.of(CursorField.of("id", row)));

            verify(jdbc).query(argThat((String sql) -> sql.contains("WHERE") &&
                    !sql.contains("cursor_")), any(SqlParameterSource.class), any(RowMapper.class));
        }

        @Test
        @DisplayName("Next page (1 field): adds single-field cursor WHERE")
        void nextPage_singleField_addsWhereClause() {
            String token = CursorToken.encode(List.of(CursorField.of("id", "9999")));

            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            when(jdbc.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                    .thenReturn(List.of());

            ClickHouseQuery.select("id", "revenue")
                    .from("events")
                    .orderBy("id", SortOrder.DESC)
                    .queryCursor(CursorRequest.nextPage(10, token), jdbc,
                            (RowMapper<String>) (rs, n) -> "row",
                            row -> List.of(CursorField.of("id", row)));

            verify(jdbc).query(argThat((String sql) -> sql.contains("id < :cursor_id")), any(SqlParameterSource.class),
                    any(RowMapper.class));
        }

        @Test
        @DisplayName("Next page (2 fields): adds tuple WHERE clause")
        void nextPage_twoFields_addsTupleWhere() {
            String token = CursorToken.encode(List.of(
                    CursorField.of("event_date", "2024-01-10"),
                    CursorField.of("id", "9999")));

            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            when(jdbc.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                    .thenReturn(List.of());

            ClickHouseQuery.select("event_date", "id")
                    .from("events")
                    .orderBy("event_date", SortOrder.DESC).orderBy("id", SortOrder.DESC)
                    .queryCursor(CursorRequest.nextPage(10, token), jdbc,
                            (RowMapper<String>) (rs, n) -> "row",
                            row -> List.of(
                                    CursorField.of("event_date", row),
                                    CursorField.of("id", row)));

            verify(jdbc).query(
                    argThat((String sql) -> sql.contains("(event_date, id) < (:cursor_event_date, :cursor_id)")),
                    any(SqlParameterSource.class), any(RowMapper.class));
        }
    }

    // ── hasNext / nextCursor detection ──────────────────────────────────

    @Nested
    @DisplayName("hasNext and nextCursor")
    @SuppressWarnings("unchecked")
    class HasNextTests {

        private static final int LIMIT = 5;

        private List<String> rows(int count) {
            List<String> list = new ArrayList<>();
            for (int i = 0; i < count; i++)
                list.add("row" + i);
            return list;
        }

        @Test
        @DisplayName("hasNext=true when DB returns limit+1 rows")
        void hasNext_true_when_fetchedLimitPlusOne() {
            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            when(jdbc.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                    .thenReturn(rows(LIMIT + 1));

            CursorPage<String> page = ClickHouseQuery.select("id").from("t")
                    .queryCursor(CursorRequest.firstPage(LIMIT), jdbc,
                            (RowMapper<String>) (rs, n) -> "row",
                            row -> List.of(CursorField.of("id", row)));

            assertTrue(page.hasNext());
            assertNotNull(page.getNextCursor());
            assertEquals(LIMIT, page.getCount());
        }

        @Test
        @DisplayName("hasNext=false and nextCursor=null on last page")
        void hasNext_false_on_last_page() {
            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            when(jdbc.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                    .thenReturn(rows(3));

            CursorPage<String> page = ClickHouseQuery.select("id").from("t")
                    .queryCursor(CursorRequest.firstPage(LIMIT), jdbc,
                            (RowMapper<String>) (rs, n) -> "row",
                            row -> List.of(CursorField.of("id", row)));

            assertFalse(page.hasNext());
            assertNull(page.getNextCursor());
            assertEquals(3, page.getCount());
        }

        @Test
        @DisplayName("Empty result returns hasNext=false and empty data")
        void emptyResult() {
            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            when(jdbc.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                    .thenReturn(List.of());

            CursorPage<String> page = ClickHouseQuery.select("id").from("t")
                    .queryCursor(CursorRequest.firstPage(10), jdbc,
                            (RowMapper<String>) (rs, n) -> "row",
                            row -> List.of(CursorField.of("id", row)));

            assertFalse(page.hasNext());
            assertTrue(page.isEmpty());
        }
    }
}
