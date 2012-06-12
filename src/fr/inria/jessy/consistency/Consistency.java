package fr.inria.jessy.consistency;

import java.util.Set;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.utils.JessyGroupManager;

public abstract class Consistency {

	protected DataStore store;
	protected TerminationCommunication terminationCommunication;
	protected JessyGroupManager manager = JessyGroupManager.getInstance();

	public Consistency(DataStore store) {
		this.store = store;
	}

	/**
	 * Returns the TerminationCommunication object
	 * 
	 * @return
	 */
	public abstract TerminationCommunication getOrCreateTerminationCommunication(Group group, Learner learner);

	/**
	 * This method checks whether the transaction with the input
	 * executionHistorycan commit or not.
	 * 
	 * @param executionHistory
	 * @return
	 */
	public abstract boolean certify(ExecutionHistory executionHistory);

	/**
	 * Returns true iff the certification of history1 and the ccertification of
	 * history2 commute according to the consistency criteria. That is
	 * cert(hist1).cert(hist2) return the same value as cert(hist2).cert(hist1).
	 * 
	 * @param history1
	 * @param history2
	 * @return
	 */
	public abstract boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2);

	/**
	 * Is called after the transaction certification outcome is true, and before
	 * changes of the transaction become permanent.
	 * 
	 * @param executionHistory
	 */
	public abstract void prepareToCommit(ExecutionHistory executionHistory);

	/**
	 * Is called after the transaction modifications have been applied to the
	 * local data store.
	 * 
	 * @param executionHistory
	 */
	public void postCommit(ExecutionHistory executionHistory) {

	}

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
			ExecutionHistory executionHistory);

}
