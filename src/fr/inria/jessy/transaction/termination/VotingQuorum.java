package fr.inria.jessy.transaction.termination;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;

import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;

/**
 * This class receives {@link Vote} from different participations in the
 * transaction termination phase. It returns {@link TransactionState.COMMITTED}
 * if it receives <code>true</code> votes from a member of all groups concerned
 * by the transaction. If it receives a <code>false</code> vote from a member of
 * a group, it will returns {@link TransactionState.ABORTED_BY_VOTING}.
 * Otherwise, it will simply waits until receiving enough votes.
 * <p>
 * A set of concerned groups is provided by {@code groups} argument of the
 * constructor.
 * 
 * @author Masoud Saeida Ardekani
 * @author Pierre Sutra
 * 
 */
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
	public synchronized void addVote(Vote vote) {
		if (vote.isCertified() == false) {
			result = TransactionState.ABORTED_BY_VOTING;
			synchronized (this) {
				notify();
			}
		} else {
			receivedVotes.remove(vote.getGroupName());
			if (receivedVotes.size() == 0) {
				result = TransactionState.COMMITTED;
				synchronized (this) {
					notify();
				}
			}
		}
	}

	/**
	 * It simply waits until it receives enough votes from members of all groups
	 * concerned by the transaction.
	 * 
	 * @return either {@link TransactionState.COMMITTED} or
	 *         {@link TransactionState.ABORTED_BY_VOTING}
	 */
	public TransactionState getTerminationResult() {
		try {
			if (result == TransactionState.COMMITTING)
				synchronized (this) {
					wait();
				}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}

}
