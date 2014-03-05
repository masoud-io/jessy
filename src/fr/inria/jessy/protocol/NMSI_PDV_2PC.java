package fr.inria.jessy.protocol;


import java.util.Set;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.termination.TwoPhaseCommit;
import fr.inria.jessy.transaction.termination.vote.Vote;

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
}
