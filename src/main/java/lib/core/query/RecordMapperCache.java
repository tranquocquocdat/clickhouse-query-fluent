package lib.core.query;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;

/**
 * Application-scoped metadata cache for Java {@code record} types.
 *
 * <p>
 * Uses {@link ClassValue} as the backing store — the JVM manages weak
 * references
 * to the key {@link Class} objects automatically, so this cache is safe in
 * environments
 * with multiple or reloading {@link ClassLoader}s (e.g. Spring DevTools, Tomcat
 * hot-deploy).
 *
 * <h3>What is cached</h3>
 * {@link RecordMeta} holds the <b>immutable</b>, reflection-resolved metadata
 * for a record:
 * <ul>
 * <li>The canonical {@link Constructor}</li>
 * <li>The {@link RecordComponent} array</li>
 * <li>A component-name → component-index lookup map</li>
 * </ul>
 * This data is resolved <b>once per record class</b> for the lifetime of the
 * application,
 * regardless of how many queries are executed against that class.
 *
 * <h3>What is NOT cached here</h3>
 * The per-query {@code rsMapping} (ResultSet column → component index) is NOT
 * stored here
 * because it depends on the SQL projection of each individual query.
 *
 * @see RecordRowMapper
 */
final class RecordMapperCache {

    private RecordMapperCache() {
    }

    /**
     * The application-wide cache.
     * {@link ClassValue#get(Class)} is thread-safe and calls
     * {@link ClassValue#computeValue(Class)} exactly once per class.
     */
    @SuppressWarnings("rawtypes")
    private static final ClassValue<RecordMeta> CACHE = new ClassValue<>() {
        @Override
        @SuppressWarnings("unchecked")
        protected RecordMeta computeValue(Class<?> type) {
            return new RecordMeta<>(type);
        }
    };

    /**
     * Return the cached {@link RecordMeta} for the given record class.
     * Computed lazily on first access; subsequent calls return the cached instance.
     *
     * @param recordType the Java record class
     * @param <T>        the record type
     * @return the cached metadata
     */
    @SuppressWarnings("unchecked")
    static <T extends Record> RecordMeta<T> get(Class<T> recordType) {
        return (RecordMeta<T>) CACHE.get(recordType);
    }

    // ────────────────────────────────────────────────────────────────────────

    /**
     * Immutable reflection metadata for one record class.
     * Instances are created once per class and shared across all queries.
     *
     * @param <T> the record type
     */
    static final class RecordMeta<T extends Record> {

        /** The canonical constructor — pre-made accessible. */
        final Constructor<T> constructor;

        /** Record components in declaration order. */
        final RecordComponent[] components;

        /**
         * Lookup map: camelCase component name → 0-based component index.
         * Used to quickly locate the right constructor argument slot
         * when building the per-query {@code rsMapping}.
         */
        final Map<String, Integer> componentIndex;

        @SuppressWarnings("unchecked")
        RecordMeta(Class<?> rawType) {
            Class<T> recordType = (Class<T>) rawType;
            this.components = recordType.getRecordComponents();

            // Build component-name → index map (immutable)
            Map<String, Integer> index = new HashMap<>(components.length * 2);
            for (int i = 0; i < components.length; i++) {
                index.put(components[i].getName(), i);
            }
            this.componentIndex = Map.copyOf(index);

            // Resolve & cache the canonical constructor
            Class<?>[] paramTypes = new Class<?>[components.length];
            for (int i = 0; i < components.length; i++) {
                paramTypes[i] = components[i].getType();
            }
            try {
                this.constructor = recordType.getDeclaredConstructor(paramTypes);
                this.constructor.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(
                        "No canonical constructor found for record " + recordType.getName(), e);
            }
        }
    }
}
