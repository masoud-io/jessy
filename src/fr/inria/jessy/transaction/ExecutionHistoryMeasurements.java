package fr.inria.jessy.transaction;

public class ExecutionHistoryMeasurements {

	/**
	 * Start time (in nanoseconds) of the transaction upon A-Delivering to a server.
	 */
	private long startCertification;
	private long applyingTransactionQueueingStartTime;
	
	private boolean voteReceiver;
	
	public void setStartCertification(long startCertification) {
		this.startCertification = startCertification;
	}

	public long getStartCertification() {
		return startCertification;
	}

	public long getApplyingTransactionQueueingStartTime() {
		return applyingTransactionQueueingStartTime;
	}

	public void setApplyingTransactionQueueingStartTime(
			long applyingTransactionQueueingStartTime) {
		this.applyingTransactionQueueingStartTime = applyingTransactionQueueingStartTime;
	}

	public boolean isVoteReceiver() {
		return voteReceiver;
	}

	public void setVoteReceiver(boolean voteReceiver) {
		this.voteReceiver = voteReceiver;
	}
	
	
}
