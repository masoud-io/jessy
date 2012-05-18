package fr.inria.jessy.consistency;

import fr.inria.jessy.transaction.ExecutionHistory;

public interface Consistency {

	public boolean certify(ExecutionHistory executionHistory);
	
	public boolean hasConflict(ExecutionHistory history1, ExecutionHistory history2);

	public void prepareToCommit(ExecutionHistory executionHistory);
}
