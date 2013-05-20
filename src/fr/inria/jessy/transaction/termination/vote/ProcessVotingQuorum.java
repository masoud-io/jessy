package fr.inria.jessy.transaction.termination.vote;

import java.util.Collection;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;

public class ProcessVotingQuorum extends VotingQuorum{

	public ProcessVotingQuorum(TransactionHandler th) {
		super(th);
	}

	@Override
	public void addVote(Vote vote) {
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

	@Override
	public TransactionState waitVoteResult(Collection<String> replicas) {
		while( result == TransactionState.COMMITTING 
				   && voters.size() < replicas.size() ){
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
					logger.debug("Has enought YES votes for  "+transactionHandler + " . Returning Committed. Groups are: " + replicas + " . voters are : " + voters);
				return TransactionState.COMMITTED;
			}
			
			if (ConstantPool.logging)
				logger.debug("DOES NOT have enought YES votes for  "+transactionHandler + " . Returning Abort_by_Voting. Groups are: " + replicas + " . voters are : " + voters);
			return TransactionState.ABORTED_BY_VOTING;
	}

}
