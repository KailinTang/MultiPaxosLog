package thread;

public class HeartBeatTracker {

    private final static int TOLERATE_FACTOR = 2;
    private final static int LEADER_TIME_OUT_WAIT = 5000;

    private final Runnable electLeaderCallBack;
    private volatile long latestReceivedTimeStamp;
    private final long maxDelayTolerance;

    public HeartBeatTracker(
            final Runnable electLeaderCallBack,
            final long latestTimeStamp,
            int heartBeatPeriod) {
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
                    electLeaderCallBack.run();
                    try {
                        // If the leader timeout is detected, we need to wait for the new leader rather than continuously run the call back
                        Thread.sleep(LEADER_TIME_OUT_WAIT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
