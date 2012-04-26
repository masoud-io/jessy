package fr.inria.jessy.transaction.termination;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Logger;

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

	private static Logger logger = Logger.getLogger(VotingQuorum.class);
	
	private TransactionState result = TransactionState.COMMITTING;
	private TransactionHandler transactionHandler;
	private Set<String> votingGroups;

	public VotingQuorum(TransactionHandler th,
			Collection<String> groups) {
		transactionHandler = th;
		votingGroups = new HashSet<String>(groups);
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
		logger.debug("adding vote for "+transactionHandler+" for "+vote.getVoterGroupName()+" with result "+vote.isAborted());
		if (vote.isAborted() == false) {
			result = TransactionState.ABORTED_BY_VOTING;
			notify();
		} else {
			votingGroups.remove(vote.getVoterGroupName());
			if (votingGroups.size() == 0) {
				result = TransactionState.COMMITTED;
				notify();
			}
		}
		logger.debug("state vote for "+transactionHandler+" "+votingGroups+" "+result);
	}

	/**
	 * It simply waits until it receives enough votes from members of all groups
	 * concerned by the transaction.
	 * 
	 * @return either {@link TransactionState.COMMITTED} or
	 *         {@link TransactionState.ABORTED_BY_VOTING}
	 */
	public synchronized TransactionState waitVoteResult() {
		logger.debug("waiting vote for "+transactionHandler);
		if (result == TransactionState.COMMITTING){
			try {
				wait();
			} catch (InterruptedException e) {
				// FIXME ignore this ?
				e.printStackTrace();
			}
		}
		return result;
	}

}
