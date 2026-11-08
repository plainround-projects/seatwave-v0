# SeatWave v0

Module 1 of Plainround’s System Design Fundamentals. Run a ticket booking service with one Java process, one embedded H2 database, and one machine. It seeds 20,000 synthetic seats. The JDK HTTP server and a small JDBC pool keep the worker and connection queues visible.

Java 25 LTS, tested on Eclipse Temurin 25.0.5+8 on November 8, 2026. The Gradle Wrapper pins 9.4.1. Dependencies checked on November 8, 2026.

| Dependency | Why it is here |
| --- | --- |
| `com.h2database:h2:2.5.252` | JDBC defines the interface, but the JDK does not ship an embedded SQL engine. |
| `com.github.sviperll:result4j:1.2.0` | The JDK has no success-or-error type that carries a typed business failure. |

From the project root, build, run the checks, and assemble the runnable distribution:

```bash
./gradlew check installDist
```

The check prints `SeatWave checks passed: 1 successful hold, 23 conflicts in the race.` It also verifies booking, expiry, duplicate confirmation, invalid inputs, and a simulated storage outage. An expected SQL-state diagnostic appears during that outage check.

Start the service with the same heap size used in the reference lab:

```bash
JAVA_OPTS='-Xms512m -Xmx512m' ./build/install/seatwave-v0/bin/seatwave-v0
```

It logs a listener at `127.0.0.1:8080` with 64 workers and 16 database connections. A second terminal can read the event or request a hold:

```bash
curl -fsS http://127.0.0.1:8080/events
curl -fsS -d 'eventId=1&seat=A-7&customer=demo_fan' http://127.0.0.1:8080/holds
```

The first response lists `Arena Night`. The second returns a generated `holdId` and `expiresInSeconds: 300`. The check above exercises confirmation using the returned ID. The conditional seat update decides the winning hold. The unique booking constraint protects confirmation independently.

The database persists under `data/`. The local H2 database defaults to the built-in `sa` user with no password. To use credentials, set `SEATWAVE_DB_USER` and `SEATWAVE_DB_PASSWORD` in the environment. For a second load machine on a private lab network, set `SEATWAVE_BIND` to the server’s private address and point the load driver at it.

## Lab 1. Find the knee

```bash
./gradlew -q load --args='http://127.0.0.1:8080 3 25 50 100' > run1.csv
./gradlew -q findKnee --args='examples/sample-run.csv'
```

The load driver writes its runtime assumptions to stderr and a CSV of rates, outcome counts and latencies to stdout. The sample fixture reports `Knee between 50 and 100 requests per second.` Use `run1.csv` for your own analysis. The short loopback run is a functional check. Use a separate driver and longer stages to measure server capacity.

The optional k6 script is in [tools/seatwave-onsale.k6.js](tools/seatwave-onsale.k6.js). The script is [not run] for this release because the Java load generator is the verified path.

## Lab 2. Compare memory policies

Stop the baseline server. Start the leak variant and run the same load below the knee from Lab 1. Repeat with the bounded variant, using a fresh local lab database or different seats for a fair comparison.

```bash
SEATWAVE_CACHE_MODE=LEAK JAVA_OPTS='-Xmx48m -Xlog:gc:file=leak.log -XX:+ExitOnOutOfMemoryError' ./build/install/seatwave-v0/bin/seatwave-v0
```

```bash
SEATWAVE_CACHE_MODE=BOUNDED JAVA_OPTS='-Xmx48m -Xlog:gc:file=bounded.log -XX:+ExitOnOutOfMemoryError' ./build/install/seatwave-v0/bin/seatwave-v0
```

Both commands start SeatWave and write GC logs to the named files. The leak holds 32 KiB per accepted hold. The bounded version keeps only IDs and expiry times, capped at 10,000. A full run to exhaustion is [not run] for this release. The time to failure depends on the heap, the number of new holds, and how much memory the database and service already use.

## Lab 3. Compare machine sizes

```bash
./gradlew -q scaleDecision --args='800 0.6 33000 1000 99.95'
```

The program prints illustrative monthly cost, safe request capacity, and the decision `START MODULE 2` for the arena case. Replace the capacity assumptions with measurements from your lab. The hourly prices are teaching inputs, not live quotes.
