package lib.core.clickhouse;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Utility for formatting dates for ClickHouse DateTime64(3, 'UTC').
 */
public final class ClickHouseDateUtil {

    private static final DateTimeFormatter CH_FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneOffset.UTC);

    private ClickHouseDateUtil() {}

    /**
     * Format an Instant to ClickHouse DateTime64 string.
     * Example: "2026-03-20 14:30:00.000"
     */
    public static String format(Instant instant) {
        if (instant == null) return null;
        return CH_FMT.format(instant);
    }
}
