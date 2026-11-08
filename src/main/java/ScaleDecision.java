/** Lab 3: buy the bigger server, or stop and start Module 2? Every input is a measured or quoted number. */
public final class ScaleDecision {

    // Cost scenarios use a 730-hour month and a 99.9% single-host ceiling for illustration.
    private static final double HOURS_PER_MONTH = 730.0;
    private static final double SINGLE_HOST_AVAILABILITY_PERCENT = 99.9;
    private static final double FULL_AVAILABILITY_PERCENT = 100.0;
    // Plan to run at most 70 percent of the knee, so a spike does not push the box over the edge.
    private static final double SAFE_FRACTION_OF_KNEE = 0.70;

    record Machine(String name, int vCpus, double dollarsPerHour) {
        double dollarsPerMonth() { return dollarsPerHour * HOURS_PER_MONTH; }
    }

    public static void main(String[] arguments) {
        if (arguments.length != 5) {
            System.err.println("usage: java ScaleDecision <kneeRpsOnSmall> <scalingEfficiency 0..1> "
                    + "<peakDemandRps> <budgetDollarsPerMonth> <availabilityTargetPercent>");
            System.exit(2);
        }
        double kneeOnSmall = Double.parseDouble(arguments[0]);
        double scalingEfficiency = Double.parseDouble(arguments[1]);
        double peakDemand = Double.parseDouble(arguments[2]);
        double budget = Double.parseDouble(arguments[3]);
        double availabilityTarget = Double.parseDouble(arguments[4]);
        if (!positive(kneeOnSmall) || !positive(scalingEfficiency) || scalingEfficiency > 1
                || !positive(peakDemand) || !positive(budget)
                || !positive(availabilityTarget) || availabilityTarget > FULL_AVAILABILITY_PERCENT) {
            System.err.println("Use positive finite inputs, efficiency in (0, 1], and availability in (0, 100].");
            System.exit(2);
        }
        // Use the module’s illustrative hourly prices so the worked examples remain reproducible.
        Machine small = new Machine("m7i.xlarge", 4, 0.2016);
        Machine large = new Machine("m7i.4xlarge", 16, 0.8064);

        double largeKnee = kneeOnSmall * (large.vCpus() / (double) small.vCpus()) * scalingEfficiency;
        for (Machine machine : new Machine[] {small, large}) {
            double knee = machine == small ? kneeOnSmall : largeKnee;
            double safeCapacity = knee * SAFE_FRACTION_OF_KNEE;
            System.out.printf("%-12s %3d vCPU  $%8.2f/month  knee %8.0f rps  safe %8.0f rps  covers peak: %s  "
                            + "fits budget: %s%n",
                    machine.name(), machine.vCpus(), machine.dollarsPerMonth(), knee, safeCapacity,
                    safeCapacity >= peakDemand ? "yes" : "no (short by " + Math.round(peakDemand - safeCapacity) + " rps)",
                    machine.dollarsPerMonth() <= budget ? "yes" : "no");
        }
        // One machine cannot promise more than the machine itself: no size fixes a single point of failure.
        boolean oneMachineMeetsAvailability = availabilityTarget <= SINGLE_HOST_AVAILABILITY_PERCENT;
        double largeSafe = largeKnee * SAFE_FRACTION_OF_KNEE;
        boolean largeFits = large.dollarsPerMonth() <= budget;
        String decision;
        if (kneeOnSmall * SAFE_FRACTION_OF_KNEE >= peakDemand && oneMachineMeetsAvailability) {
            decision = "STOP HERE: the small machine already covers peak. Spend nothing more.";
        } else if (largeFits && largeSafe >= peakDemand && oneMachineMeetsAvailability) {
            decision = "BUY THE BIGGER SERVER: it covers peak inside budget, and one machine meets the availability target.";
        } else {
            decision = "START MODULE 2: no single machine in budget covers peak with headroom"
                    + (oneMachineMeetsAvailability ? "." : ", and one machine cannot meet the availability target.");
        }
        System.out.println(decision);
    }

    private static boolean positive(double value) {
        return Double.isFinite(value) && value > 0;
    }
}
