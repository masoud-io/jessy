package fr.inria.jessy.transaction;

public class ExecutionHistoryMeasurements {

	/**
	 * Start time (in nanoseconds) of the transaction upon A-Delivering to a server.
	 */
	private long startCertificationTime;
	private long applyingTransactionQueueingStartTime;
	
	private boolean voteReceiver;
	
	public void setStartCertificationTime(long startCertificationTime) {
		this.startCertificationTime = startCertificationTime;
	}

	public long getStartCertificationTime() {
		return startCertificationTime;
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
