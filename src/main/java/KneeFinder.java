import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Lab 1: reads the LoadGenerator CSV and names the knee of the curve. */
public final class KneeFinder {

    // The knee is the first stage where p99 more than doubles against the best healthy stage...
    private static final double P99_GROWTH_LIMIT = 2.0;
    // ...or errors pass 1 percent: by then users are already being refused or timing out.
    private static final double ERROR_PERCENT_LIMIT = 1.0;
    // Ignore the first stage's p99 as a baseline: it includes JIT warm-up, which is not load.
    private static final int WARM_UP_STAGES = 1;
    private static final int EXPECTED_COLUMNS = 8;

    record Stage(int targetRate, double errorPercent, long p50Millis, long p99Millis) { }

    public static void main(String[] arguments) throws IOException {
        if (arguments.length != 1) {
            System.err.println("usage: java KneeFinder <load-results.csv>");
            System.exit(2);
        }
        List<Stage> stages = readStages(Path.of(arguments[0]));
        if (stages.size() <= WARM_UP_STAGES) {
            System.err.println("need at least " + (WARM_UP_STAGES + 1) + " stages");
            System.exit(2);
        }
        long healthyP99 = stages.get(WARM_UP_STAGES).p99Millis();
        Stage lastHealthy = stages.get(WARM_UP_STAGES);
        for (int stageIndex = WARM_UP_STAGES; stageIndex < stages.size(); stageIndex++) {
            Stage stage = stages.get(stageIndex);
            boolean latencyBroke = stage.p99Millis() > P99_GROWTH_LIMIT * Math.max(healthyP99, 1);
            boolean errorsBroke = stage.errorPercent() > ERROR_PERCENT_LIMIT;
            if (latencyBroke || errorsBroke) {
                System.out.printf("Knee between %d and %d requests per second.%n",
                        lastHealthy.targetRate(), stage.targetRate());
                System.out.printf("Last healthy stage: p50=%d ms, p99=%d ms, errors=%.1f%%%n",
                        lastHealthy.p50Millis(), lastHealthy.p99Millis(), lastHealthy.errorPercent());
                System.out.printf("First broken stage: p50=%d ms, p99=%d ms, errors=%.1f%% (%s)%n",
                        stage.p50Millis(), stage.p99Millis(), stage.errorPercent(),
                        latencyBroke ? "p99 jumped" : "errors rose");
                return;
            }
            healthyP99 = Math.min(healthyP99, stage.p99Millis());
            lastHealthy = stage;
        }
        System.out.println("No knee found: raise the top stage rate and run again.");
    }

    private static List<Stage> readStages(Path csvFile) throws IOException {
        List<Stage> stages = new ArrayList<>();
        List<String> lines = Files.readAllLines(csvFile);
        for (int lineIndex = 1; lineIndex < lines.size(); lineIndex++) {
            String[] columns = lines.get(lineIndex).split(",");
            if (columns.length != EXPECTED_COLUMNS) {
                continue; // Skip log noise or partial lines rather than crash on them.
            }
            stages.add(new Stage(Integer.parseInt(columns[0]), Double.parseDouble(columns[5]),
                    Long.parseLong(columns[6]), Long.parseLong(columns[7])));
        }
        return stages;
    }
}
