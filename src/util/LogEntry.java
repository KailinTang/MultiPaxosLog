package util;

public class LogEntry {

    // one log entry will be chosen if and only if acceptedProposal = Integer.MAX_VALUE
    private int acceptedProposal;
    private String acceptedValue;

    private int numberOfReceivedPrepareResponse;
    private int numberOfReceivedAcceptResponse;

    public LogEntry(int acceptedProposal, String acceptedValue) {
        this.acceptedProposal = acceptedProposal;
        this.acceptedValue = acceptedValue;
        this.numberOfReceivedPrepareResponse = 0;
        this.numberOfReceivedAcceptResponse = 0;
    }

    public int getAcceptedProposal() {
        return acceptedProposal;
    }

    public void setAcceptedProposal(int acceptedProposal) {
        this.acceptedProposal = acceptedProposal;
    }

    public String getAcceptedValue() {
        return acceptedValue;
    }

    public void setAcceptedValue(String acceptedValue) {
        this.acceptedValue = acceptedValue;
    }

    public int getNumberOfReceivedPrepareResponse() {
        return numberOfReceivedPrepareResponse;
    }

    public void setNumberOfReceivedPrepareResponse(int numberOfReceivedPrepareResponse) {
        this.numberOfReceivedPrepareResponse = numberOfReceivedPrepareResponse;
    }
}
