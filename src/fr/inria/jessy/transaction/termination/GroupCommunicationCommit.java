package fr.inria.jessy.transaction.termination;

import java.util.Set;

import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.consistency.Consistency.ConcernedKeysTarget;

public class GroupCommunicationCommit extends AtomicCommit{

	public GroupCommunicationCommit(DistributedTermination termination) {
		super(termination);
	}

	/**
	 * A thread starts certifying the transaction in the TerminateTransactionRequestMessage.
	 * 
	 * @param msg TerminateTransactionRequestMessage containing the transaction need to be certified
	 * @return true if this transaction can be certified, false if it need to preemptively abort.
	 */
	@Override
	public boolean proceedToCertifyAndVote(TerminateTransactionRequestMessage msg){
		try{
				/*
				 * First, à la P-Store.
				 */
				synchronized (atomicDeliveredMessages) {
					while (true) {

						boolean isConflicting = false;

						for (TerminateTransactionRequestMessage n : atomicDeliveredMessages) {
							if (n.equals(msg)) {
								break;
							}
							if (!jessy.getConsistency().certificationCommute(n.getExecutionHistory(), msg.getExecutionHistory())) 
							{
								isConflicting = true;
								break;									
							}
						}
						if (isConflicting)
							atomicDeliveredMessages.wait();
						else
							break;
					}
				}
		}
		catch(Exception ex)
		{ 
			ex.printStackTrace();

		}
		return true;
	}
	
	@Override
	public void setVoters(TerminateTransactionRequestMessage msg ,Set<String> voteReceivers, Set<String> voteSenders){
			voteReceivers =	jessy.partitioner.resolveNames(jessy
					.getConsistency().getConcerningKeys(
							msg.getExecutionHistory(),
							ConcernedKeysTarget.RECEIVE_VOTES));
			
			voteSenders =jessy.partitioner.resolveNames(jessy
					.getConsistency().getConcerningKeys(
							msg.getExecutionHistory(),
							ConcernedKeysTarget.SEND_VOTES)); 
	}
	
	@Override
	public void sendVote(VoteMessage voteMessage, TerminateTransactionRequestMessage msg){
		voteMulticast.sendVote(voteMessage, msg.getExecutionHistory().isCertifyAtCoordinator(), msg.getExecutionHistory().getCoordinatorSwid(), msg.getExecutionHistory().getCoordinatorHost());
	}
}
