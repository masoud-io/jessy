package fr.inria.jessy.consistency;

import java.util.Set;

import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;

public abstract class Consistency {

	protected DataStore store;

	public Consistency(DataStore store) {
		this.store = store;
	}

	/**
	 * This method checks whether the transaction with the input
	 * executionHistorycan commit or not.
	 * 
	 * @param executionHistory
	 * @return
	 */
	public abstract boolean certify(ExecutionHistory executionHistory);

	/**
	 * Returns true if history1 has a conflict with history2 according to the
	 * consistency criterion peropertis.
	 * <p>
	 * For example, under Snapshot Isolation, history1 has conflict with
	 * history2 if their writeset intersection is not empty.
	 * 
	 * @param history1
	 * @param history2
	 * @return
	 */
	public abstract boolean hasConflict(ExecutionHistory history1,
			ExecutionHistory history2);

	/**
	 * This method is called after the transaction certification outcome is
	 * true, and before changes of the transaction become permanent.
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
			ExecutionHistory executionHistory);
}
