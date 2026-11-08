import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Optional memory experiment. The database stays authoritative for every hold. */
final class RecentHolds {
    enum Mode { OFF, LEAK, BOUNDED }

    // Lab 2 uses 32 KiB per accepted hold to make the growth visible on a small heap.
    private static final int SNAPSHOT_BYTES = 32 * 1024;
    // The fixed solution keeps at most ten thousand IDs, even when none have expired.
    private static final int MAX_ENTRIES = 10_000;
    private final Mode mode;
    private final Map<String, byte[]> snapshots = new LinkedHashMap<>();
    private final LinkedHashMap<String, Instant> recentIds = new LinkedHashMap<>();

    RecentHolds(Mode mode) {
        this.mode = mode;
    }

    synchronized void remember(String holdId, Instant expiresAt) {
        switch (mode) {
            case OFF -> { }
            case LEAK -> snapshots.put(holdId, new byte[SNAPSHOT_BYTES]);
            case BOUNDED -> rememberBounded(holdId, expiresAt, Instant.now());
        }
    }

    private void rememberBounded(String holdId, Instant expiresAt, Instant now) {
        // Expiry order can differ from insertion order when concurrent requests finish out of order.
        recentIds.values().removeIf(expiry -> !expiry.isAfter(now));
        recentIds.put(holdId, expiresAt);
        if (recentIds.size() > MAX_ENTRIES) recentIds.pollFirstEntry();
    }
}
