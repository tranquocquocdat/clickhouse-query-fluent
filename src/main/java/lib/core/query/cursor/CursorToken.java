package lib.core.query.cursor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Internal utility — encodes/decodes a cursor (list of CursorField) to/from
 * an opaque Base64-JSON string token.
 *
 * <p>
 * Format of the JSON before encoding:
 * 
 * <pre>
 * [{"c":"event_date","v":"2024-01-10"},{"c":"id","v":"9999"}]
 * </pre>
 * 
 * Values are written as strings; the receiving side passes them through
 * {@link org.springframework.jdbc.core.namedparam.MapSqlParameterSource}
 * which handles type coercion.
 *
 * <p>
 * <b>Internal API — not for application use.</b>
 */
public final class CursorToken {

    private CursorToken() {
    }

    // ── Encoding ──────────────────────────────────────────────────────────

    /**
     * Encodes a list of cursor fields to an opaque Base64-JSON string.
     *
     * @param fields list must not be empty
     * @return opaque cursor token
     */
    public static String encode(List<CursorField> fields) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("cursor fields must not be empty");
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            CursorField f = fields.get(i);
            if (i > 0)
                sb.append(",");
            sb.append("{\"c\":\"")
                    .append(escapeJson(f.getColumn()))
                    .append("\",\"v\":\"")
                    .append(escapeJson(String.valueOf(f.getValue())))
                    .append("\"}");
        }
        sb.append("]");
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    // ── Decoding ──────────────────────────────────────────────────────────

    /**
     * Decodes an opaque cursor token back to a list of cursor fields.
     *
     * @param token Base64-encoded JSON token
     * @return ordered list of CursorField
     * @throws IllegalArgumentException if the token is malformed
     */
    public static List<CursorField> decode(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("cursor token must not be blank");
        }
        String json;
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(token);
            json = new String(bytes, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid cursor token (not valid Base64): " + token, e);
        }

        // Simple hand-rolled parser to avoid pulling in Jackson/Gson
        List<CursorField> result = new ArrayList<>();
        // Strip outer [ ]
        json = json.trim();
        if (!json.startsWith("[") || !json.endsWith("]")) {
            throw new IllegalArgumentException("Malformed cursor token: " + json);
        }
        json = json.substring(1, json.length() - 1).trim();
        if (json.isEmpty())
            return result;

        // Split on },{
        String[] entries = splitEntries(json);
        for (String entry : entries) {
            // entry looks like: {"c":"event_date","v":"2024-01-10"}
            String col = extractValue(entry, "\"c\":");
            String val = extractValue(entry, "\"v\":");
            result.add(CursorField.of(col, val));
        }
        return result;
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static String[] splitEntries(String inner) {
        // Split on },{ boundary
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '{')
                depth++;
            else if (c == '}') {
                depth--;
                if (depth == 0) {
                    parts.add(inner.substring(start, i + 1).trim());
                    // skip comma separator
                    start = i + 2;
                }
            }
        }
        return parts.toArray(new String[0]);
    }

    /**
     * Extract the string value after a key like {@code "c":} inside a JSON object.
     */
    private static String extractValue(String entry, String key) {
        int keyIdx = entry.indexOf(key);
        if (keyIdx < 0)
            throw new IllegalArgumentException("Key " + key + " not found in: " + entry);
        int valueStart = entry.indexOf('"', keyIdx + key.length()) + 1;
        int valueEnd = entry.indexOf('"', valueStart);
        return unescapeJson(entry.substring(valueStart, valueEnd));
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String unescapeJson(String s) {
        return s.replace("\\\"", "\"").replace("\\\\", "\\");
    }
}
