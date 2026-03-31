package lib.core.query.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlParamsTest {

    enum Status { ACTIVE, INACTIVE }

    @Test
    @DisplayName("set basic values")
    void setBasic() {
        MapSqlParameterSource params = SqlParams.of()
                .set("id", 123)
                .set("name", "test")
                .build();
                
        assertEquals(123, params.getValue("id"));
        assertEquals("test", params.getValue("name"));
    }
    
    @Test
    @DisplayName("setOrDefault falls back if null")
    void setOrDefault() {
        MapSqlParameterSource params = SqlParams.of()
                .setOrDefault("val1", null, "default1")
                .setOrDefault("val2", "actual", "default2")
                .build();
                
        assertEquals("default1", params.getValue("val1"));
        assertEquals("actual", params.getValue("val2"));
    }
    
    @Test
    @DisplayName("setEnum sets name or null")
    void setEnum() {
        MapSqlParameterSource params = SqlParams.of()
                .setEnum("status1", Status.ACTIVE)
                .setEnum("status2", null)
                .build();
                
        assertEquals("ACTIVE", params.getValue("status1"));
        assertNull(params.getValue("status2"));
    }
    
    @Test
    @DisplayName("setTimestamp converts Instant")
    void setTimestamp() {
        Instant now = Instant.now();
        MapSqlParameterSource params = SqlParams.of()
                .setTimestamp("time1", now)
                .setTimestamp("time2", null)
                .build();
                
        assertEquals(java.sql.Timestamp.from(now), params.getValue("time1"));
        assertNull(params.getValue("time2"));
    }
    
    @Test
    @DisplayName("setArray converts List to Array")
    void setArray() {
        List<String> list = List.of("a", "b");
        MapSqlParameterSource params = SqlParams.of()
                .setArray("arr1", list, String.class)
                .setArray("arr2", null, String.class)
                .build();
                
        assertArrayEquals(new String[]{"a", "b"}, (String[]) params.getValue("arr1"));
        assertArrayEquals(new String[]{}, (String[]) params.getValue("arr2"));
    }

    @Test
    @DisplayName("setIfNotNull skips null values")
    void setIfNotNull() {
        MapSqlParameterSource params = SqlParams.of()
                .set("id", 1)
                .setIfNotNull("note", null)
                .setIfNotNull("name", "test")
                .build();

        assertEquals(1, params.getValue("id"));
        assertFalse(params.hasValue("note"));
        assertEquals("test", params.getValue("name"));
    }
}
