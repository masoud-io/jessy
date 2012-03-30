package fr.inria.jessy.transaction.termination;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;

import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.termination.DistributedTermination.TerminationResult;

//TODO COMMENT ME
public class VotingQuorum {

	TerminationResult result = TerminationResult.UNKNOWN;

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
			result = TerminationResult.ABORTED;
			notify();
		} else {
			receivedVotes.remove(vote.getGroupName());
			if (receivedVotes.size() == 0) {
				result = TerminationResult.COMMITTED;
				notify();
			}
		}
	}

	public TerminationResult getTerminationResult() {
		try {
			if (result == TerminationResult.UNKNOWN)
				wait();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}

}
