package util;

public class LogEntrySlotManager {

    private static final int INITIAL_ARRAY_CAPACITY = 10;

    private LogEntry[] logEntryArray;
    private final int size;

    private int firstUnchosenIndex; // the smallest log index that have not been chosen
    private int lastLogIndex;   // the largest entry for which this server has accepted a proposal
    private int minProposal;    // the number of the smallest proposal this server will accept for any log entry

    public LogEntrySlotManager() {
        this.logEntryArray = new LogEntry[INITIAL_ARRAY_CAPACITY];
        this.size = 0;
        this.firstUnchosenIndex = 0;
        this.lastLogIndex = 0;
        this.minProposal = 0;
    }

    private void logEntryArrayExpand() {
        final int previousSize = this.logEntryArray.length;
        LogEntry[] newLogEntryArray = new LogEntry[previousSize * 2];
        for (int i = 0; i < previousSize; i++) {
            newLogEntryArray[i] = logEntryArray[i];
        }
        this.logEntryArray = newLogEntryArray;
    }
}
