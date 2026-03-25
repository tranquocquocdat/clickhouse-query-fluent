package lib.core.clickhouse;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ClickHouseInsertTest {

    @Test
    @DisplayName("Builds INSERT SQL correctly")
    void buildsInsertSql() {
        String sql = ClickHouseInsert.into("users")
                .columns("id", "first_name", "last_name", "created_at")
                .toSql();

        assertEquals("INSERT INTO users (id, first_name, last_name, created_at) VALUES (:id, :firstName, :lastName, :createdAt)", sql);
    }

    @Test
    @DisplayName("Executes single insert")
    void executeSingle() {
        NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
        MapSqlParameterSource params = new MapSqlParameterSource("id", 1);
        
        ClickHouseInsert.into("users")
                .columns("id")
                .execute(jdbc, 1, id -> params);

        verify(jdbc).update("INSERT INTO users (id) VALUES (:id)", params);
    }

    @Test
    @DisplayName("Executes batch insert")
    void executeBatch() {
        NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
        List<Integer> ids = List.of(1, 2);
        
        ClickHouseInsert.into("users")
                .columns("id")
                .executeBatch(jdbc, ids, id -> new MapSqlParameterSource("id", id));

        verify(jdbc).batchUpdate(eq("INSERT INTO users (id) VALUES (:id)"), any(SqlParameterSource[].class));
    }
    
    @Test
    @DisplayName("toCamelCase converts correctly")
    void toCamelCase() {
        assertEquals("userId", ClickHouseInsert.toCamelCase("user_id"));
        assertEquals("createdAt", ClickHouseInsert.toCamelCase("created_at"));
        assertEquals("someLongColumnName", ClickHouseInsert.toCamelCase("some_long_column_name"));
        assertEquals("id", ClickHouseInsert.toCamelCase("id"));
    }
}
