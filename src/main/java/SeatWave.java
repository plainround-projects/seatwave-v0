import com.github.sviperll.result4j.Result;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static java.net.HttpURLConnection.*;

/**
 * SeatWave version 0: one Java process, one embedded database, one machine.
 * Four operations: list events, view seats, hold a seat, confirm a booking.
 */
public final class SeatWave {

    // ---- Configuration: every limit has a name, so nothing is a magic number. ----
    private static final System.Logger LOGGER = System.getLogger(SeatWave.class.getName());
    // Bind to loopback by default. Use a private lab address for a separate load driver.
    private static final String BIND_ADDRESS = System.getenv().getOrDefault("SEATWAVE_BIND", "127.0.0.1");
    private static final int HTTP_PORT = Integer.getInteger("seatwave.port", 8080);
    private static final int MAX_TCP_PORT = 65535;
    private static final long BYTES_PER_MIB = 1024L * 1024;
    // Worker threads cap how many requests run at once (Module 4 explains how to size this).
    private static final int WORKER_THREADS = Integer.getInteger("seatwave.workerThreads", 64);
    // Database connections are fewer than workers on purpose: the pool is a visible queue.
    private static final int DATABASE_CONNECTIONS = Integer.getInteger("seatwave.dbConnections", 16);
    private static final String DATABASE_URL = System.getProperty(
            "seatwave.dbUrl", "jdbc:h2:file:./data/seatwave;WRITE_DELAY=0");
    private static final String DATABASE_USER = System.getenv().getOrDefault("SEATWAVE_DB_USER", "sa");
    private static final String DATABASE_PASSWORD = System.getenv().getOrDefault("SEATWAVE_DB_PASSWORD", "");
    private static final Duration HOLD_DURATION = Duration.ofMinutes(
            Long.getLong("seatwave.holdMinutes", 5));
    // Waiting longer than this for a connection means we are saturated: refuse instead of queueing forever.
    private static final Duration CONNECTION_WAIT_LIMIT = Duration.ofMillis(
            Long.getLong("seatwave.connectionWaitMillis", 500));
    private static final int MAX_REQUEST_BODY_BYTES = 1024;
    private static final int SECTION_COUNT = 20;
    private static final int SEATS_PER_SECTION = 1000;
    private static final long DEMO_EVENT_ID = 1L;
    private static final int SEED_BATCH_SIZE = 1000;

    private static final Pattern SEAT_LABEL_PATTERN = Pattern.compile("[A-T]-[0-9]{1,4}");
    private static final Pattern SECTION_PATTERN = Pattern.compile("[A-T]");
    private static final Pattern CUSTOMER_PATTERN = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private static final Pattern HOLD_ID_PATTERN = Pattern.compile("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}");

    private static final AtomicLong REQUESTS_IN_FLIGHT = new AtomicLong();
    private static final AtomicLong REQUESTS_REFUSED = new AtomicLong();

    // ---- Data shapes ----
    record Event(long eventId, String name, long startsAtEpochMillis) { }

    /** A seat is always in exactly one of three states. The compiler checks we handle all three. */
    sealed interface SeatState permits Available, Held, Confirmed { }
    record Available() implements SeatState { }
    record Held(String holdId, long expiresAtEpochMillis) implements SeatState { }
    record Confirmed(String bookingId) implements SeatState { }

    record Seat(long eventId, String seatLabel, String section, SeatState state) { }

    record HttpReply(int statusCode, String jsonBody) { }

    /** Thrown when the service chooses to refuse work rather than risk overload or a wrong answer. */
    static final class ServiceBusyException extends Exception {
        private static final long serialVersionUID = 1L;
        ServiceBusyException(String message) { super(message); }
    }

    sealed interface BookingFailure permits InvalidInput, MethodNotAllowed, UnknownRoute,
            RequestTooLarge, SeatUnavailable, HoldMissingOrExpired, ServiceBusy, StorageUnavailable { }
    record InvalidInput(String detail) implements BookingFailure { }
    record MethodNotAllowed(String allowed) implements BookingFailure { }
    record UnknownRoute() implements BookingFailure { }
    record RequestTooLarge() implements BookingFailure { }
    record SeatUnavailable() implements BookingFailure { }
    record HoldMissingOrExpired() implements BookingFailure { }
    record ServiceBusy() implements BookingFailure { }
    record StorageUnavailable() implements BookingFailure { }

    // ---- A deliberately small connection pool: the JDK has none, and seeing the queue is the lesson. ----
    static final class ConnectionPool implements AutoCloseable {
        private final BlockingQueue<Connection> idleConnections;

        ConnectionPool(int size) throws SQLException {
            idleConnections = new ArrayBlockingQueue<>(size);
            try {
                for (int connectionNumber = 0; connectionNumber < size; connectionNumber++) {
                    idleConnections.add(DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD));
                }
            } catch (SQLException openFailure) {
                close();
                throw openFailure;
            }
        }

        Connection borrow() throws ServiceBusyException {
            try {
                Connection connection = idleConnections.poll(CONNECTION_WAIT_LIMIT.toMillis(), TimeUnit.MILLISECONDS);
                if (connection == null) {
                    throw new ServiceBusyException("database connections exhausted");
                }
                return connection;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new ServiceBusyException("interrupted while waiting for a connection");
            }
        }

        void giveBack(Connection connection) {
            idleConnections.offer(connection);
        }

        @Override
        public void close() {
            for (Connection connection : idleConnections) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    LOGGER.log(System.Logger.Level.WARNING, "Connection close failed, SQL state {0}", closeFailure.getSQLState());
                }
            }
        }
    }

    // ---- Database layer ----
    static final class SeatRepository {
        private final ConnectionPool connectionPool;

        SeatRepository(ConnectionPool connectionPool) {
            this.connectionPool = connectionPool;
        }

        void createSchemaAndSeedIfMissing() throws ServiceBusyException, SQLException {
            Connection connection = connectionPool.borrow();
            try {
                try (var statement = connection.createStatement()) {
                    statement.execute("CREATE TABLE IF NOT EXISTS events (event_id BIGINT PRIMARY KEY, "
                            + "name VARCHAR(120) NOT NULL, starts_at BIGINT NOT NULL)");
                    // The status column is the single source of truth for a seat. Java never decides alone.
                    statement.execute("CREATE TABLE IF NOT EXISTS seats (event_id BIGINT NOT NULL, seat_label VARCHAR(16) NOT NULL, "
                            + "section VARCHAR(4) NOT NULL, status VARCHAR(12) NOT NULL, hold_id VARCHAR(40), "
                            + "hold_customer VARCHAR(64), hold_expires_at BIGINT, booking_id VARCHAR(40), "
                            + "PRIMARY KEY (event_id, seat_label))");
                    // Second guard: even a bug in the UPDATE logic cannot create two bookings for one seat.
                    statement.execute("CREATE TABLE IF NOT EXISTS bookings (booking_id VARCHAR(40) PRIMARY KEY, "
                            + "event_id BIGINT NOT NULL, seat_label VARCHAR(16) NOT NULL, customer VARCHAR(64) NOT NULL, "
                            + "confirmed_at BIGINT NOT NULL, CONSTRAINT one_booking_per_seat UNIQUE (event_id, seat_label))");
                    statement.execute("CREATE INDEX IF NOT EXISTS seats_by_section ON seats (event_id, section)");
                }
                // The event and its seats commit together. A failed seed can be retried on startup.
                try (PreparedStatement findEvent = connection.prepareStatement(
                        "SELECT event_id FROM events WHERE event_id = ?")) {
                    findEvent.setLong(1, DEMO_EVENT_ID);
                    try (ResultSet rows = findEvent.executeQuery()) {
                        if (rows.next()) return;
                    }
                }
                seedDemoEvent(connection);
            } finally {
                connectionPool.giveBack(connection);
            }
        }

        private static void seedDemoEvent(Connection connection) throws SQLException {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement insertEvent = connection.prepareStatement(
                         "INSERT INTO events (event_id, name, starts_at) VALUES (?, ?, ?)");
                 PreparedStatement insertSeat = connection.prepareStatement(
                         "INSERT INTO seats (event_id, seat_label, section, status) VALUES (?, ?, ?, 'AVAILABLE')")) {
                insertEvent.setLong(1, DEMO_EVENT_ID);
                insertEvent.setString(2, "Arena Night");
                insertEvent.setLong(3, System.currentTimeMillis() + Duration.ofDays(30).toMillis());
                insertEvent.executeUpdate();
                int seatsInBatch = 0;
                for (int sectionIndex = 0; sectionIndex < SECTION_COUNT; sectionIndex++) {
                    String section = String.valueOf((char) ('A' + sectionIndex));
                    for (int seatNumber = 1; seatNumber <= SEATS_PER_SECTION; seatNumber++) {
                        insertSeat.setLong(1, DEMO_EVENT_ID);
                        insertSeat.setString(2, section + "-" + seatNumber);
                        insertSeat.setString(3, section);
                        insertSeat.addBatch();
                        seatsInBatch++;
                        if (seatsInBatch == SEED_BATCH_SIZE) {
                            insertSeat.executeBatch();
                            seatsInBatch = 0;
                        }
                    }
                }
                insertSeat.executeBatch();
                connection.commit();
            } catch (SQLException seedFailure) {
                connection.rollback();
                throw seedFailure;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        }

        List<Event> listEvents() throws ServiceBusyException, SQLException {
            Connection connection = connectionPool.borrow();
            try (PreparedStatement query = connection.prepareStatement(
                    "SELECT event_id, name, starts_at FROM events ORDER BY starts_at");
                 ResultSet rows = query.executeQuery()) {
                List<Event> events = new ArrayList<>();
                while (rows.next()) {
                    events.add(new Event(rows.getLong("event_id"), rows.getString("name"), rows.getLong("starts_at")));
                }
                return events;
            } finally {
                connectionPool.giveBack(connection);
            }
        }

        List<Seat> listSeats(long eventId, String section) throws ServiceBusyException, SQLException {
            long now = System.currentTimeMillis();
            Connection connection = connectionPool.borrow();
            try (PreparedStatement query = connection.prepareStatement(
                    "SELECT seat_label, section, status, hold_id, hold_expires_at, booking_id FROM seats "
                            + "WHERE event_id = ? AND section = ? ORDER BY seat_label")) {
                query.setLong(1, eventId);
                query.setString(2, section);
                try (ResultSet rows = query.executeQuery()) {
                    List<Seat> seats = new ArrayList<>(SEATS_PER_SECTION);
                    while (rows.next()) {
                        seats.add(new Seat(eventId, rows.getString("seat_label"), rows.getString("section"),
                                toSeatState(rows, now)));
                    }
                    return seats;
                }
            } finally {
                connectionPool.giveBack(connection);
            }
        }

        private static SeatState toSeatState(ResultSet row, long now) throws SQLException {
            String status = row.getString("status");
            if ("CONFIRMED".equals(status)) {
                return new Confirmed(row.getString("booking_id"));
            }
            long expiresAt = row.getLong("hold_expires_at");
            // An expired hold is shown as available: expiry is lazy, so no background thread is needed.
            if ("HELD".equals(status) && expiresAt > now) {
                return new Held(row.getString("hold_id"), expiresAt);
            }
            return new Available();
        }

        /** One UPDATE decides whether this request won the hold. */
        Result<String, BookingFailure> holdSeat(long eventId, String seatLabel, String customer) throws ServiceBusyException, SQLException {
            long now = System.currentTimeMillis();
            String holdId = UUID.randomUUID().toString();
            Connection connection = connectionPool.borrow();
            try (PreparedStatement update = connection.prepareStatement(
                    """
                    UPDATE seats
                    SET status = 'HELD', hold_id = ?, hold_customer = ?, hold_expires_at = ?
                    WHERE event_id = ? AND seat_label = ?
                      AND (status = 'AVAILABLE' OR (status = 'HELD' AND hold_expires_at <= ?))
                    """)) {
                update.setString(1, holdId);
                update.setString(2, customer);
                update.setLong(3, now + HOLD_DURATION.toMillis());
                update.setLong(4, eventId);
                update.setString(5, seatLabel);
                update.setLong(6, now);
                // The database row lock makes this check-and-set atomic: two racers cannot both see "1 row updated".
                return update.executeUpdate() == 1
                        ? Result.success(holdId)
                        : Result.error(new SeatUnavailable());
            } finally {
                connectionPool.giveBack(connection);
            }
        }

        /** The guarded update and unique booking constraint enforce one booking per seat. */
        Result<String, BookingFailure> confirmBooking(String holdId) throws ServiceBusyException, SQLException {
            long now = System.currentTimeMillis();
            String bookingId = UUID.randomUUID().toString();
            Connection connection = connectionPool.borrow();
            boolean previousAutoCommit = true;
            try {
                previousAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                String seatLabel;
                long eventId;
                String customer;
                try (PreparedStatement find = connection.prepareStatement(
                        "SELECT event_id, seat_label, hold_customer FROM seats "
                                + "WHERE hold_id = ? AND status = 'HELD' AND hold_expires_at > ?")) {
                    find.setString(1, holdId);
                    find.setLong(2, now);
                    try (ResultSet rows = find.executeQuery()) {
                        if (!rows.next()) {
                            connection.rollback();
                            return Result.error(new HoldMissingOrExpired());
                        }
                        eventId = rows.getLong("event_id");
                        seatLabel = rows.getString("seat_label");
                        customer = rows.getString("hold_customer");
                    }
                }
                try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE seats SET status = 'CONFIRMED', booking_id = ? "
                                + "WHERE hold_id = ? AND status = 'HELD' AND hold_expires_at > ?")) {
                    update.setString(1, bookingId);
                    update.setString(2, holdId);
                    update.setLong(3, now);
                    // Re-check the condition in the UPDATE itself: the SELECT above may be stale by now.
                    if (update.executeUpdate() != 1) {
                        connection.rollback();
                        return Result.error(new HoldMissingOrExpired());
                    }
                }
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO bookings (booking_id, event_id, seat_label, customer, confirmed_at) "
                                + "VALUES (?, ?, ?, ?, ?)")) {
                    insert.setString(1, bookingId);
                    insert.setLong(2, eventId);
                    insert.setString(3, seatLabel);
                    insert.setString(4, customer);
                    insert.setLong(5, now);
                    insert.executeUpdate();
                }
                connection.commit();
                return Result.success(bookingId);
            } catch (SQLException confirmFailure) {
                connection.rollback();
                throw confirmFailure;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
                connectionPool.giveBack(connection);
            }
        }
    }

    // ---- HTTP layer ----
    private final SeatRepository seatRepository;
    private final RecentHolds recentHolds = new RecentHolds(
            RecentHolds.Mode.valueOf(System.getenv().getOrDefault("SEATWAVE_CACHE_MODE", "OFF")));

    SeatWave(SeatRepository seatRepository) {
        this.seatRepository = seatRepository;
    }

    public static void main(String[] arguments) throws IOException, SQLException, ServiceBusyException {
        if (arguments.length != 0 || HTTP_PORT < 0 || HTTP_PORT > MAX_TCP_PORT) {
            throw new IllegalArgumentException("Run SeatWave without arguments and set a valid seatwave.port if needed.");
        }
        RunningServer server = start(HTTP_PORT);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        LOGGER.log(System.Logger.Level.INFO, "SeatWave v0 listening at {0}:{1} (workers={2}, dbConnections={3})",
                BIND_ADDRESS, Integer.toString(server.port()), WORKER_THREADS, DATABASE_CONNECTIONS);
    }

    record RunningServer(HttpServer server, ExecutorService workers, ConnectionPool pool) implements AutoCloseable {
        int port() { return server.getAddress().getPort(); }

        @Override
        public void close() {
            server.stop(1);
            workers.shutdown();
            pool.close();
        }
    }

    static RunningServer start(int port) throws IOException, SQLException, ServiceBusyException {
        ConnectionPool pool = new ConnectionPool(DATABASE_CONNECTIONS);
        ExecutorService workers = Executors.newFixedThreadPool(WORKER_THREADS);
        try {
            SeatRepository seatRepository = new SeatRepository(pool);
            seatRepository.createSchemaAndSeedIfMissing();
            SeatWave service = new SeatWave(seatRepository);
            HttpServer server = HttpServer.create(new InetSocketAddress(BIND_ADDRESS, port), 0);
            server.setExecutor(workers);
            server.createContext("/events", exchange -> service.handle(exchange, "GET", service::routeEvents));
            server.createContext("/holds", exchange -> service.handle(exchange, "POST", service::routeHolds));
            server.createContext("/bookings", exchange -> service.handle(exchange, "POST", service::routeBookings));
            server.createContext("/health", exchange -> service.handle(exchange, "GET", SeatWave::health));
            server.start();
            return new RunningServer(server, workers, pool);
        } catch (IOException | SQLException | ServiceBusyException startupFailure) {
            workers.shutdown();
            pool.close();
            throw startupFailure;
        }
    }

    @FunctionalInterface
    interface Route {
        Result<HttpReply, BookingFailure> apply(HttpExchange exchange) throws IOException, SQLException, ServiceBusyException;
    }

    private void handle(HttpExchange exchange, String method, Route route) throws IOException {
        long startNanos = System.nanoTime();
        REQUESTS_IN_FLIGHT.incrementAndGet();
        try {
            HttpReply reply = switch (dispatch(exchange, method, route)) {
                case Result.Success<HttpReply, BookingFailure>(HttpReply response) -> response;
                case Result.Error<HttpReply, BookingFailure>(BookingFailure failure) -> errorReply(exchange, failure);
            };
            byte[] body = reply.jsonBody().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            double elapsedMillis = (System.nanoTime() - startNanos) / (double) TimeUnit.MILLISECONDS.toNanos(1);
            exchange.getResponseHeaders().set("Server-Timing", "app;dur=" + elapsedMillis);
            exchange.sendResponseHeaders(reply.statusCode(), body.length);
            try (OutputStream responseStream = exchange.getResponseBody()) {
                responseStream.write(body);
            }
        } finally {
            REQUESTS_IN_FLIGHT.decrementAndGet();
            exchange.close();
        }
    }

    private static Result<HttpReply, BookingFailure> dispatch(HttpExchange exchange, String method, Route route)
            throws IOException {
        if (!method.equals(exchange.getRequestMethod())) return Result.error(new MethodNotAllowed(method));
        try {
            return route.apply(exchange);
        } catch (ServiceBusyException busy) {
            // Refuse work under saturation. A retryable response protects the booking invariant.
            REQUESTS_REFUSED.incrementAndGet();
            return Result.error(new ServiceBusy());
        } catch (SQLException databaseFailure) {
            LOGGER.log(System.Logger.Level.ERROR, "Storage unavailable, SQL state {0}", databaseFailure.getSQLState());
            return Result.error(new StorageUnavailable());
        }
    }

    private static HttpReply errorReply(HttpExchange exchange, BookingFailure failure) {
        return switch (failure) {
            case InvalidInput(String detail) -> new HttpReply(HTTP_BAD_REQUEST, errorJson(detail));
            case UnknownRoute() -> new HttpReply(HTTP_NOT_FOUND, errorJson("unknown path"));
            case MethodNotAllowed(String allowed) -> {
                exchange.getResponseHeaders().set("Allow", allowed);
                yield new HttpReply(HTTP_BAD_METHOD, errorJson("method must be " + allowed));
            }
            case RequestTooLarge() -> new HttpReply(HTTP_ENTITY_TOO_LARGE, errorJson("request body too large"));
            case SeatUnavailable() -> new HttpReply(HTTP_CONFLICT, errorJson("seat not available"));
            case HoldMissingOrExpired() -> new HttpReply(HTTP_CONFLICT, errorJson("hold missing or expired"));
            case ServiceBusy() -> new HttpReply(HTTP_UNAVAILABLE, errorJson("busy, retry shortly"));
            case StorageUnavailable() -> new HttpReply(HTTP_UNAVAILABLE, errorJson("storage unavailable"));
        };
    }

    private Result<HttpReply, BookingFailure> routeEvents(HttpExchange exchange) throws SQLException, ServiceBusyException {
        String path = exchange.getRequestURI().getPath();
        if ("/events".equals(path)) {
            return Result.success(new HttpReply(HTTP_OK, eventsJson(seatRepository.listEvents())));
        }
        String[] pathParts = path.split("/", -1);
        // The only nested route is /events/{eventId}/seats.
        if (pathParts.length != 4 || !"events".equals(pathParts[1]) || !"seats".equals(pathParts[3])) {
            return Result.error(new UnknownRoute());
        }
        long eventId = positiveEventId(pathParts[2]);
        if (eventId == 0) return Result.error(new InvalidInput("invalid eventId"));
        return switch (parseForm(exchange.getRequestURI().getRawQuery())) {
            case Result.Success<Map<String, String>, BookingFailure>(var query) -> {
                String section = query.get("section");
                if (!matches(section, SECTION_PATTERN)) yield Result.error(new InvalidInput("invalid section"));
                yield Result.success(new HttpReply(HTTP_OK, seatsJson(seatRepository.listSeats(eventId, section))));
            }
            case Result.Error<Map<String, String>, BookingFailure>(var failure) -> Result.error(failure);
        };
    }

    private Result<HttpReply, BookingFailure> routeHolds(HttpExchange exchange)
            throws IOException, SQLException, ServiceBusyException {
        if (!"/holds".equals(exchange.getRequestURI().getPath())) return Result.error(new UnknownRoute());
        return switch (readForm(exchange)) {
            case Result.Success<Map<String, String>, BookingFailure>(var form) -> hold(form);
            case Result.Error<Map<String, String>, BookingFailure>(var failure) -> Result.error(failure);
        };
    }

    private Result<HttpReply, BookingFailure> hold(Map<String, String> form) throws SQLException, ServiceBusyException {
        long eventId = positiveEventId(form.get("eventId"));
        String seatLabel = form.get("seat");
        String customer = form.get("customer");
        if (eventId == 0) return Result.error(new InvalidInput("invalid eventId"));
        if (!matches(seatLabel, SEAT_LABEL_PATTERN)) return Result.error(new InvalidInput("invalid seat"));
        if (!matches(customer, CUSTOMER_PATTERN)) return Result.error(new InvalidInput("invalid customer"));
        return seatRepository.holdSeat(eventId, seatLabel, customer).map(holdId -> {
            recentHolds.remember(holdId, Instant.now().plus(HOLD_DURATION));
            return new HttpReply(HTTP_CREATED, "{\"holdId\":\"" + holdId + "\",\"expiresInSeconds\":"
                    + HOLD_DURATION.toSeconds() + "}");
        });
    }

    private Result<HttpReply, BookingFailure> routeBookings(HttpExchange exchange)
            throws IOException, SQLException, ServiceBusyException {
        if (!"/bookings".equals(exchange.getRequestURI().getPath())) return Result.error(new UnknownRoute());
        return switch (readForm(exchange)) {
            case Result.Success<Map<String, String>, BookingFailure>(var form) -> {
                String holdId = form.get("holdId");
                if (!matches(holdId, HOLD_ID_PATTERN)) yield Result.error(new InvalidInput("invalid holdId"));
                yield seatRepository.confirmBooking(holdId)
                        .map(bookingId -> new HttpReply(HTTP_CREATED, "{\"bookingId\":\"" + bookingId + "\"}"));
            }
            case Result.Error<Map<String, String>, BookingFailure>(var failure) -> Result.error(failure);
        };
    }

    private static Result<HttpReply, BookingFailure> health(HttpExchange exchange) {
        if (!"/health".equals(exchange.getRequestURI().getPath())) return Result.error(new UnknownRoute());
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        return Result.success(new HttpReply(HTTP_OK, "{\"inFlight\":" + REQUESTS_IN_FLIGHT.get()
                + ",\"refused\":" + REQUESTS_REFUSED.get()
                + ",\"heapUsedMb\":" + heap.getUsed() / BYTES_PER_MIB
                + ",\"heapMaxMb\":" + heap.getMax() / BYTES_PER_MIB + "}"));
    }

    private static Result<Map<String, String>, BookingFailure> readForm(HttpExchange exchange) throws IOException {
        try (InputStream bodyStream = exchange.getRequestBody()) {
            // One extra byte distinguishes a complete request from a truncated form.
            byte[] bytes = bodyStream.readNBytes(MAX_REQUEST_BODY_BYTES + 1);
            if (bytes.length > MAX_REQUEST_BODY_BYTES) return Result.error(new RequestTooLarge());
            return parseForm(new String(bytes, StandardCharsets.UTF_8));
        }
    }

    private static Result<Map<String, String>, BookingFailure> parseForm(String encoded) {
        Map<String, String> fields = new HashMap<>();
        if (encoded == null || encoded.isEmpty()) return Result.success(fields);
        try {
            for (String pair : encoded.split("&", -1)) {
                int separatorIndex = pair.indexOf('=');
                if (separatorIndex <= 0) return Result.error(new InvalidInput("malformed form field"));
                String name = URLDecoder.decode(pair.substring(0, separatorIndex), StandardCharsets.UTF_8);
                String value = URLDecoder.decode(pair.substring(separatorIndex + 1), StandardCharsets.UTF_8);
                if (fields.putIfAbsent(name, value) != null) return Result.error(new InvalidInput("duplicate form field"));
            }
            return Result.success(fields);
        } catch (IllegalArgumentException invalidEncoding) {
            return Result.error(new InvalidInput("malformed form encoding"));
        }
    }

    private static boolean matches(String value, Pattern allowed) {
        return value != null && allowed.matcher(value).matches();
    }

    private static long positiveEventId(String value) {
        if (value == null) return 0;
        try {
            return Math.max(0, Long.parseLong(value));
        } catch (NumberFormatException invalidNumber) {
            return 0;
        }
    }

    // ---- JSON output (hand-written to stay on the standard library; all values are validated or generated) ----
    private static String eventsJson(List<Event> events) {
        StringBuilder json = new StringBuilder("[");
        for (Event event : events) {
            if (json.length() > 1) {
                json.append(',');
            }
            json.append("{\"eventId\":").append(event.eventId())
                    .append(",\"name\":\"").append(event.name())
                    .append("\",\"startsAt\":").append(event.startsAtEpochMillis()).append('}');
        }
        return json.append(']').toString();
    }

    private static String seatsJson(List<Seat> seats) {
        StringBuilder json = new StringBuilder("[");
        for (Seat seat : seats) {
            if (json.length() > 1) {
                json.append(',');
            }
            json.append("{\"seat\":\"").append(seat.seatLabel()).append("\",\"state\":\"")
                    .append(stateName(seat.state())).append("\"}");
        }
        return json.append(']').toString();
    }

    private static String stateName(SeatState state) {
        // Never expose who holds a seat or the hold id: that would let another client confirm the hold.
        if (state instanceof Held) {
            return "held";
        }
        if (state instanceof Confirmed) {
            return "sold";
        }
        return "available";
    }

    private static String errorJson(String message) {
        return "{\"error\":\"" + message + "\"}";
    }
}
