package fr.inria.jessy.protocol;


import java.util.Set;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.TwoPhaseCommit;
import fr.inria.jessy.transaction.termination.vote.Vote;
import fr.inria.jessy.transaction.termination.vote.VotePiggyback;
import fr.inria.jessy.vector.PartitionDependenceVector;

/**
 * This class implements Non-Monotonic Snapshot Isolation consistency criterion.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class NMSI_PDV_2PC extends NMSI_PDV_GC {

	
	public NMSI_PDV_2PC(JessyGroupManager m, DataStore dataStore) {
		super(m, dataStore);
	}

	/**
	 * Coordinator needs to only wait for the vote from the 2PC manager. 
	 * 	
	 */
	@Override
	public Set<String> getVotersToJessyProxy(
			Set<String> termincationRequestReceivers,
			ExecutionHistory executionHistory) {
		termincationRequestReceivers.clear();
		termincationRequestReceivers.add(TwoPhaseCommit.getCoordinatorId(executionHistory,manager.getPartitioner()));
		return termincationRequestReceivers;
	}
	
	@Override
	public void voteReceived(Vote vote) {
		
		/*
		 * if vote.getVotePiggyBack() is null, it means that it is preemptively aborted in DistributedTermination, and
		 * DistributedTermination sets votePiggyback to null. 
		 *  
		 */
		if (vote.getVotePiggyBack()==null ){
			return;
		}
		
		super.voteReceived(vote);
	}
	
	@Override
	public void quorumReached(TerminateTransactionRequestMessage msg,TransactionState state, Vote vote){
		/*
		 * Transaction manager needs to collect all votes, computes the final vector, and send it to everybody.
		 * 
		 */
		PartitionDependenceVector<String> commitVC = receivedVectors.get(vote.getTransactionHandler().getId());
		vote.setVotePiggyBack(new VotePiggyback(commitVC));
	}
}
