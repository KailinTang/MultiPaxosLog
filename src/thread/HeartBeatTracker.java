package thread;

public class HeartBeatTracker {

    private final static int TOLERATE_FACTOR = 4;
    private final static int LEADER_TIME_OUT_WAIT = 5000;

    private final Runnable increaseViewNumberCallBack;
    private final Runnable electLeaderCallBack;
    private volatile long latestReceivedTimeStamp;
    private final long maxDelayTolerance;

    public HeartBeatTracker(
            final Runnable increaseViewNumberCallBack,
            final Runnable electLeaderCallBack,
            final long latestTimeStamp,
            int heartBeatPeriod
    ) {
        this.increaseViewNumberCallBack = increaseViewNumberCallBack;
        this.electLeaderCallBack = electLeaderCallBack;
        this.latestReceivedTimeStamp = latestTimeStamp;
        this.maxDelayTolerance = heartBeatPeriod * TOLERATE_FACTOR;
    }

    public void start() {
        new Thread(new TrackerHandler()).start();
    }

    public long getLatestReceivedTimeStamp() {
        return latestReceivedTimeStamp;
    }

    public void setLatestReceivedTimeStamp(final long latestReceivedTimeStamp) {
        this.latestReceivedTimeStamp = latestReceivedTimeStamp;
    }

    public class TrackerHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                final long currentTimeStamp = System.currentTimeMillis();
                if (currentTimeStamp - latestReceivedTimeStamp > maxDelayTolerance) {
                    System.out.println("Heart Beat Timeout!");
                    increaseViewNumberCallBack.run();
                    electLeaderCallBack.run();
                    setLatestReceivedTimeStamp(System.currentTimeMillis());
                }
            }
        }
    }
}
