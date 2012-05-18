package fr.inria.jessy.consistency;

import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;

public abstract class Consistency {

	protected DataStore store;

	public Consistency(DataStore store) {
		this.store = store;
	}

	/**
	 * This method checks whether this transaction can commit or not.
	 * 
	 * @param executionHistory
	 * @return
	 */
	public abstract boolean certify(ExecutionHistory executionHistory);

	public abstract boolean hasConflict(ExecutionHistory history1,
			ExecutionHistory history2);

	public abstract void prepareToCommit(ExecutionHistory executionHistory);
}
