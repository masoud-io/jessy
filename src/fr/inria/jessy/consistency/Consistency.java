package fr.inria.jessy.consistency;

import fr.inria.jessy.transaction.ExecutionHistory;

public interface Consistency {
	public boolean certify(ExecutionHistory executionHistory);
}
