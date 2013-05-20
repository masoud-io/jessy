package fr.inria.jessy.transaction.termination.vote;

import java.util.Collection;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;

public class GroupVotingQuorum extends VotingQuorum{

	public GroupVotingQuorum(TransactionHandler th) {
		super(th);
	}

	/**
	 * Add a received {@code vote}.
	 * <p>
	 * It returns {@link TransactionState.COMMITTED} if it receives
	 * <code>true</code> votes from a member of all groups concerned by the
	 * transaction. If it receives a <code>false</code> vote from a member of a
	 * group, it will returns {@link TransactionState.ABORTED_BY_VOTING}.
	 * 
	 * @param vote
	 */
	@Override
	public synchronized void addVote(Vote vote) {
		if (ConstantPool.logging)
			logger.debug("adding vote for "+transactionHandler+" for "+vote.getVoterEntityName()+" with result "+vote.isCommitted());
		
		if (vote.isCommitted() == false) {
			result = TransactionState.ABORTED_BY_VOTING;
		} else{
			voters.add(vote.getVoterEntityName());
		}
		
		notified=true;
		notifyAll();
	}

	/**

	 * 
	 * @return either {@link TransactionState.COMMITTED} or
	 *         {@link TransactionState.ABORTED_BY_VOTING}
	 */
	public synchronized TransactionState waitVoteResult(Collection<String> groups) {

		while( result == TransactionState.COMMITTING
			   && voters.size() < groups.size() ){
			try {
				notified=false;
				wait(ConstantPool.JESSY_VOTING_QUORUM_TIMEOUT);
				if (!notified){
					return TransactionState.ABORTED_BY_TIMEOUT;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		if( result == TransactionState.COMMITTING ){
			if (ConstantPool.logging)
				logger.debug("Has enought YES votes for  "+transactionHandler + " . Returning Committed. Groups are: " + groups + " . voters are : " + voters);
			return TransactionState.COMMITTED;
		}
		
		if (ConstantPool.logging)
			logger.debug("DOES NOT have enought YES votes for  "+transactionHandler + " . Returning Abort_by_Voting. Groups are: " + groups + " . voters are : " + voters);
		return TransactionState.ABORTED_BY_VOTING;
	}

}
