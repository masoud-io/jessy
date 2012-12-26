package fr.inria.jessy.transaction.termination;

import java.util.concurrent.LinkedBlockingQueue;

import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;

/**
 * This class is used for applying the transactions to the data store SEQUENTIALLY.
 * For certain consistencies (i.e., NMSI-GMUVector, US, SI, PSI), update transactions should be applied sequentially.  
 * 
 * @author Masoud Saeida Ardekani
 *
 */
public class ApplyTransactionsToDataStore implements Runnable {

	private LinkedBlockingQueue<TerminateTransactionRequestMessage> queue;
	
	private DistributedTermination distributedTermination;

	public ApplyTransactionsToDataStore(
			DistributedTermination distributedTermination) {
		this.distributedTermination = distributedTermination;
		queue = new LinkedBlockingQueue<TerminateTransactionRequestMessage>();
	}

	public void run() {

		while (true) {
			try {

				TerminateTransactionRequestMessage msg = queue.take();

				distributedTermination.applyingTransactionQueueingTime
						.add(System.nanoTime()
								- msg.getExecutionHistory()
										.getApplyingTransactionQueueingStartTime());

				distributedTermination.handleTerminationResult(msg);

				distributedTermination.measureCertificatioinTime(msg, msg
						.getExecutionHistory().isVoteReceiver());

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public void addToQueue(TerminateTransactionRequestMessage msg) {
		queue.add(msg);

	}
	
	public void removeFromQueue(TerminateTransactionRequestMessage msg){
		queue.remove(msg);
	}
}
