package fr.inria.jessy.transaction.termination.vote;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;

/**
 * This class receives {@link Vote} from different participations in the
 * transaction termination phase. It returns {@link TransactionState.COMMITTED}
 * if it receives <code>true</code> votes from all replica/groups concerned
 * by the transaction. If it receives a <code>false</code> vote from a member of
 * a group or a process, it will returns {@link TransactionState.ABORTED_BY_VOTING}.
 * Otherwise, it will simply waits until receiving enough votes.
 * <p>
 * 
 * @author Masoud Saeida Ardekani
 * @author Pierre Sutra
 * 
 */
public class VotingQuorum {
	protected static Logger logger = Logger.getLogger(VotingQuorum.class);
	
	protected TransactionState result = TransactionState.COMMITTING;
	protected TransactionHandler transactionHandler;
	
	protected Set<String> receivedVoters;
	protected boolean notified=false;

	public VotingQuorum(TransactionHandler th){
		transactionHandler = th;
		receivedVoters = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());
	}
	
	public void addVote(Vote vote) {
//		System.out.println("Adding vote for " + vote.getTransactionHandler().getId() + " from " + vote.getVoterEntityName());
		synchronized(this){

			if (vote.isCommitted() == false) {
				result = TransactionState.ABORTED_BY_VOTING;
			} else{
				receivedVoters.add(vote.getVoterEntityName());
			}

			notified=true;
			notifyAll();		
		}
	}

	public TransactionState waitVoteResult(Collection<String> allVoters) {
		synchronized(this){
			while( result == TransactionState.COMMITTING 
					&& receivedVoters.size() < allVoters.size() ){
//				System.out.println("Still waiting for votes " + transactionHandler.getId() + " all : " + allVoters + " but voters : " +receivedVoters );
				try {
					notified=false;
					this.wait(ConstantPool.JESSY_VOTING_QUORUM_TIMEOUT);
					if (!notified){
						return TransactionState.ABORTED_BY_TIMEOUT;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
			
			if( result == TransactionState.COMMITTING ){
				if (ConstantPool.logging)
					logger.error("Has enought YES votes for  "+transactionHandler + " . Returning Committed. Groups are: " + allVoters + " . voters are : " + receivedVoters);
				return TransactionState.COMMITTED;
			}
			
			if (ConstantPool.logging)
				logger.error("DOES NOT have enought YES votes for  "+transactionHandler + " . Returning Abort_by_Voting. Groups are: " + allVoters + " . voters are : " + receivedVoters);
			return TransactionState.ABORTED_BY_VOTING;
	}
	
	public Collection<String> getReceivedVoters() {
		return receivedVoters;
	}
}
