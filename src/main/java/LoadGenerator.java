import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Open-model step load generator: sends a fixed rate per second no matter how slow the server is,
 * which is how a real on-sale behaves. A closed model (wait for each reply) would hide the overload.
 */
public final class LoadGenerator {

    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(2);
    private static final double HOLD_SHARE = 0.3;
    private static final int SECTION_COUNT = 20;
    private static final int SEATS_PER_SECTION = 1000;
    private static final int PERCENT = 100;
    private static final int MEDIAN_PERCENTILE = 50;
    private static final int TAIL_PERCENTILE = 99;
    // The caller gives async HTTP requests time to finish after the last scheduled send.
    private static final int DRAIN_TIMEOUT_MULTIPLIER = 5;
    private static final int SYNTHETIC_CUSTOMER_RANGE = 1_000_000;

    record StageResult(int targetRate, long sent, long ok, long conflicts, long errors, long p50Millis,
                       long p99Millis) { }

    public static void main(String[] arguments) throws Exception {
        String baseUrl = arguments.length > 0 ? arguments[0] : "http://localhost:8080";
        URI target = URI.create(baseUrl);
        if (!("http".equals(target.getScheme()) || "https".equals(target.getScheme()))
                || target.getHost() == null || target.getRawQuery() != null || target.getRawFragment() != null
                || target.getRawUserInfo() != null || !(target.getPath().isEmpty() || target.getPath().equals("/"))) {
            throw new IllegalArgumentException("Supply an HTTP(S) origin, such as http://127.0.0.1:8080");
        }
        baseUrl = baseUrl.replaceFirst("/$", "");
        int stageSeconds = arguments.length > 1 ? Integer.parseInt(arguments[1]) : 20;
        List<Integer> stageRates = new ArrayList<>();
        for (int argumentIndex = 2; argumentIndex < arguments.length; argumentIndex++) {
            stageRates.add(Integer.parseInt(arguments[argumentIndex]));
        }
        if (stageRates.isEmpty()) {
            stageRates = List.of(50, 100, 200, 400, 800, 1600);
        }
        if (stageSeconds <= 0 || stageRates.stream().anyMatch(rate -> rate <= 0 || (long) rate * stageSeconds > Integer.MAX_VALUE)) {
            throw new IllegalArgumentException("stage durations and rates must be positive and fit in the request counter");
        }
        System.err.printf("Driver: %s, %s, Java %s, %d available processors, max heap %d bytes%n",
                System.getProperty("os.name"), System.getProperty("os.arch"), Runtime.version(),
                Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory());
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(CLIENT_TIMEOUT).build()) {
            System.out.println("target_rps,sent,ok_2xx,conflict_409,errors,error_pct,p50_ms,p99_ms");
            for (int targetRate : stageRates) {
                StageResult result = runStage(client, baseUrl, targetRate, stageSeconds);
                double errorPercent = result.sent() == 0 ? 0 : PERCENT * (double) result.errors() / result.sent();
                System.out.printf(java.util.Locale.ROOT, "%d,%d,%d,%d,%d,%.1f,%d,%d%n", result.targetRate(), result.sent(), result.ok(),
                        result.conflicts(), result.errors(), errorPercent, result.p50Millis(), result.p99Millis());
            }
        }
    }

    private static StageResult runStage(HttpClient client, String baseUrl, int targetRate, int stageSeconds)
            throws InterruptedException {
        long totalRequests = (long) targetRate * stageSeconds;
        ConcurrentLinkedQueue<Long> latenciesMillis = new ConcurrentLinkedQueue<>();
        AtomicLong okCount = new AtomicLong();
        AtomicLong conflictCount = new AtomicLong();
        AtomicLong errorCount = new AtomicLong();
        CountDownLatch finished = new CountDownLatch((int) totalRequests);
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / targetRate;
        AtomicLong scheduled = new AtomicLong();
        ticker.scheduleAtFixedRate(() -> {
            if (scheduled.getAndIncrement() >= totalRequests) {
                return;
            }
            HttpRequest request = nextRequest(baseUrl);
            long sentAtNanos = System.nanoTime();
            client.sendAsync(request, HttpResponse.BodyHandlers.discarding()).whenComplete((response, failure) -> {
                // Timeouts count as errors AND as latency at the timeout: dropping them would flatter p99.
                latenciesMillis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sentAtNanos));
                if (failure != null || response.statusCode() >= 500) {
                    errorCount.incrementAndGet();
                } else if (response.statusCode() == 409) {
                    conflictCount.incrementAndGet();
                } else if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    okCount.incrementAndGet();
                } else {
                    errorCount.incrementAndGet();
                }
                finished.countDown();
            });
        }, 0, intervalNanos, TimeUnit.NANOSECONDS);
        try {
            if (!finished.await(stageSeconds + CLIENT_TIMEOUT.toSeconds() * DRAIN_TIMEOUT_MULTIPLIER, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Load driver could not finish the stage. Reduce the rate and retry.");
            }
        } finally {
            ticker.shutdownNow();
        }
        List<Long> sorted = new ArrayList<>(latenciesMillis);
        Collections.sort(sorted);
        return new StageResult(targetRate, totalRequests, okCount.get(), conflictCount.get(),
                errorCount.get() + (totalRequests - sorted.size()), percentile(sorted, MEDIAN_PERCENTILE), percentile(sorted, TAIL_PERCENTILE));
    }

    private static HttpRequest nextRequest(String baseUrl) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String section = String.valueOf((char) ('A' + random.nextInt(SECTION_COUNT)));
        if (random.nextDouble() < HOLD_SHARE) {
            String seatLabel = section + "-" + (1 + random.nextInt(SEATS_PER_SECTION));
            String form = "eventId=1&seat=" + seatLabel + "&customer=fan" + random.nextInt(SYNTHETIC_CUSTOMER_RANGE);
            return HttpRequest.newBuilder(URI.create(baseUrl + "/holds")).timeout(CLIENT_TIMEOUT)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(form)).build();
        }
        return HttpRequest.newBuilder(URI.create(baseUrl + "/events/1/seats?section=" + section))
                .timeout(CLIENT_TIMEOUT).GET().build();
    }

    private static long percentile(List<Long> sortedValues, int percentileRank) {
        if (sortedValues.isEmpty()) {
            return 0;
        }
        int rankIndex = (int) Math.ceil(percentileRank / (double) PERCENT * sortedValues.size()) - 1;
        return sortedValues.get(Math.max(0, rankIndex));
    }
}
