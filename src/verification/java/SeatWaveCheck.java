import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static java.net.HttpURLConnection.*;

public final class SeatWaveCheck {
    private static final String CHECK_DATABASE = "jdbc:h2:mem:seatwave-check";
    private static final int CONCURRENT_HOLDS = 24;
    private static final int OVERSIZED_BYTES = 2048;
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Pattern HOLD_ID = Pattern.compile("\"holdId\":\"([^\"]+)\"");

    public static void main(String[] arguments) throws Exception {
        System.setProperty("seatwave.dbUrl", CHECK_DATABASE);
        try (var server = SeatWave.start(0); var client = HttpClient.newHttpClient()) {
            URI base = URI.create("http://127.0.0.1:" + server.port());
            expect(HTTP_OK, send(client, base, "/events", null));
            expect(HTTP_OK, send(client, base, "/events/1/seats?section=A", null));
            expect(HTTP_BAD_METHOD, send(client, base, "/holds", null));
            expect(HTTP_NOT_FOUND, send(client, base, "/holds/extra", "eventId=1"));
            expect(HTTP_BAD_REQUEST, send(client, base, "/holds", "eventId=0&seat=A-7&customer=demo"));
            expect(HTTP_BAD_REQUEST, send(client, base, "/holds", "eventId=%ZZ"));
            expect(HTTP_ENTITY_TOO_LARGE, send(client, base, "/holds", "x".repeat(OVERSIZED_BYTES)));
            expect(HTTP_CONFLICT, send(client, base, "/bookings", "holdId=00000000-0000-0000-0000-000000000000"));

            var hold = send(client, base, "/holds", "eventId=1&seat=A-7&customer=demo");
            expect(HTTP_CREATED, hold);
            String holdId = extractHoldId(hold);
            var booking = send(client, base, "/bookings", "holdId=" + holdId);
            expect(HTTP_CREATED, booking);
            expect(HTTP_CONFLICT, send(client, base, "/bookings", "holdId=" + holdId));

            var expiringHold = send(client, base, "/holds", "eventId=1&seat=A-8&customer=demo");
            expect(HTTP_CREATED, expiringHold);
            try (var database = DriverManager.getConnection(CHECK_DATABASE, "sa", "");
                 var expire = database.prepareStatement("UPDATE seats SET hold_expires_at = 0 WHERE hold_id = ?")) {
                expire.setString(1, extractHoldId(expiringHold));
                if (expire.executeUpdate() != 1) throw new AssertionError("Expected one hold to expire");
            }
            expect(HTTP_CONFLICT, send(client, base, "/bookings", "holdId=" + extractHoldId(expiringHold)));
            expect(HTTP_CREATED, send(client, base, "/holds", "eventId=1&seat=A-8&customer=next_fan"));

            List<CompletableFuture<HttpResponse<String>>> contenders = IntStream.range(0, CONCURRENT_HOLDS)
                    .mapToObj(contenderIndex -> client.sendAsync(
                            request(base, "/holds", "eventId=1&seat=C-42&customer=fan_" + contenderIndex),
                            HttpResponse.BodyHandlers.ofString()))
                    .toList();
            var outcomes = contenders.stream().map(CompletableFuture::join).toList();
            long holdsCreated = outcomes.stream().filter(outcome -> outcome.statusCode() == HTTP_CREATED).count();
            long conflicts = outcomes.stream().filter(outcome -> outcome.statusCode() == HTTP_CONFLICT).count();
            if (holdsCreated != 1 || conflicts != CONCURRENT_HOLDS - 1) {
                throw new AssertionError("Expected one winner, got " + holdsCreated + " holds and " + conflicts + " conflicts");
            }
            try (var database = DriverManager.getConnection(CHECK_DATABASE, "sa", "");
                 var statement = database.createStatement()) {
                statement.execute("DROP TABLE events");
            }
            expect(HTTP_UNAVAILABLE, send(client, base, "/events", null));
            System.out.printf("SeatWave checks passed: %d successful hold, %d conflicts in the race.%n", holdsCreated, conflicts);
        }
    }

    private static String extractHoldId(HttpResponse<String> hold) {
        var match = HOLD_ID.matcher(hold.body());
        if (!match.find()) throw new AssertionError("Response did not contain a hold ID");
        return match.group(1);
    }

    private static HttpRequest request(URI base, String path, String form) {
        var builder = HttpRequest.newBuilder(base.resolve(path)).timeout(REQUEST_TIMEOUT);
        return (form == null ? builder.GET() : builder
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))).build();
    }

    private static HttpResponse<String> send(HttpClient client, URI base, String path, String form) throws Exception {
        return client.send(request(base, path, form), HttpResponse.BodyHandlers.ofString());
    }

    private static void expect(int expected, HttpResponse<String> response) {
        if (response.statusCode() != expected) {
            throw new AssertionError(response.uri() + " expected " + expected + " but got " + response.statusCode());
        }
    }
}
