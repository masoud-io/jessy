package fr.inria.jessy.transaction.termination;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;

import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;

//TODO COMMENT ME
public class VotingQuorum {

	TransactionState result = TransactionState.COMMITTING;

	TransactionHandler transactionHandler;
	CopyOnWriteArraySet<String> receivedVotes;

	public VotingQuorum(TransactionHandler transactionHandler,
			Collection<String> groups) {
		this.transactionHandler = transactionHandler;
		receivedVotes = new CopyOnWriteArraySet<String>();
		receivedVotes.addAll(groups);
	}

	public synchronized void addVote(Vote vote) {
		if (vote.isCertified() == false) {
			result = TransactionState.ABORTED_BY_VOTING;
			synchronized(this){
				notify();
			}
		} else {
			receivedVotes.remove(vote.getGroupName());
			if (receivedVotes.size() == 0) {
				result = TransactionState.COMMITTED;
				synchronized(this){
					notify();
				}
			}
		}
	}

	public TransactionState getTerminationResult() {
		try {
			if (result == TransactionState.COMMITTING)
				synchronized(this){
					wait();
				}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}

}
