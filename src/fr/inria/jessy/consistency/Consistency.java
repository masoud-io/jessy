package fr.inria.jessy.consistency;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.util.Set;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.termination.Vote;

public abstract class Consistency {

	public static enum ConcernedKeysTarget {
		TERMINATION_CAST, RECEIVE_VOTES, SEND_VOTES
	};

	protected DataStore store;
	protected TerminationCommunication terminationCommunication;
	protected JessyGroupManager manager = JessyGroupManager.getInstance();

	protected static boolean votePiggybackRequired = false;

	public Consistency(DataStore store) {
		this.store = store;
	}

	/**
	 * Returns the TerminationCommunication object
	 * 
	 * @return
	 */
	public abstract TerminationCommunication getOrCreateTerminationCommunication(
			Group group, Learner learner);

	/**
	 * This method checks whether the transaction with the input
	 * executionHistorycan commit or not.
	 * 
	 * @param executionHistory
	 * @return
	 */
	public abstract boolean certify(ExecutionHistory executionHistory);

	/**
	 * Returns true iff the certification of history1 and the certification of
	 * history2 commute according to the consistency criteria. That is
	 * certification(hist1).certification(hist2) return the same value as 
	 * certification(hist2).certification(hist1).
	 * 
	 * @param history1
	 * @param history2
	 * @return
	 */
	public abstract boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2);
	
	/**
	 * Returns true iff the transaction can be applied to the data store 
	 * without waiting for other concurrent transactions to be applied as they are atomically delivered.
	 * 
	 * @return
	 */
	public abstract boolean applyingTransactionCommute();

	/**
	 * Is called after the transaction certification outcome is true, and before
	 * changes of the transaction become permanent.
	 * 
	 * @param executionHistory
	 */
	public abstract void prepareToCommit(ExecutionHistory executionHistory);

	/**
	 * Returns the set of keys that are concerned by the transaction. Concerning
	 * keys are those keys that the replicas holding them should certify the
	 * transaction. For example, for Non-monotonic Snapshot Isolation, it's just
	 * the union of the keys of the writeset and createset. For Serializability,
	 * its the union of keys of readset, writeset, and createset.
	 * 
	 * <p>
	 * The transaction will later be sent to all the replicas that replicate the
	 * concerning keys.
	 * 
	 * @param executionHistory
	 * @return
	 */
	public abstract Set<String> getConcerningKeys(
			ExecutionHistory executionHistory, ConcernedKeysTarget target);

	/**
	 * Is called after the transaction modifications have been applied to the
	 * local data store. Can be used for example for propagating information
	 * after the transaction has been committed (E.g.,
	 * {@code ParallelSnapshotIsalation}).
	 * 
	 * @param executionHistory
	 */
	public void postCommit(ExecutionHistory executionHistory) {
		return;
	}

	/**
	 * Is called after a vote is received, and before adding it to the voting
	 * quorums.
	 */
	public void voteReceived(Vote vote) {
		return;
	}

	/**
	 * Returns the Vote containing the certification result at a particular
	 * jessy instance.
	 * 
	 * <p>
	 * A transaction can commit, if it receives a <i>yes</i> vote from at least
	 * one jessy instance that replicates an object conerned by the transaction.
	 * 
	 * @param executionHistory
	 * @return
	 */
	public Vote createCertificationVote(ExecutionHistory executionHistory) {
		/*
		 * First, it needs to run the certification test on the received
		 * execution history. A blind write always succeeds.
		 */

		boolean isAborted = executionHistory.getTransactionType() == BLIND_WRITE
				|| certify(executionHistory);

		return new Vote(executionHistory.getTransactionHandler(), isAborted,
				JessyGroupManager.getInstance().getMyGroup().name(), null);
	}

	public boolean isVotePiggybackRequired() {
		return votePiggybackRequired;
	}

	/**
	 * Returns a set of groups which the coordinator should expect to receive
	 * votes from. Normally, it is the same as the set of destinations of the
	 * {@link TerminateTransactionRequestMessage}. However, for
	 * {@link SnapshotIsolationWithMulticast} and {@link SnapshotIsolationWithBroadcast},
	 * while the destination is all jessy server instances, the voters to the
	 * coordinator are only those that have modified something inside the
	 * transaction. This is because under snapshot isolation, the algorithm is
	 * not genuine. Thus, while the transaction should be send to all server
	 * instances, the coordinator does not need to receive yes from all jessy
	 * server instance.
	 * 
	 * 
	 * @param termincationRequestReceivers
	 *            the set used for sending the
	 *            {@link TerminateTransactionRequestMessage}
	 * @return
	 */
	public Set<String> getVotersToCoordinator(
			Set<String> termincationRequestReceivers,
			ExecutionHistory executionHistory) {
		return termincationRequestReceivers;
	}

}
