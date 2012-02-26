package fr.inria.jessy.consistency;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.transaction.ExecutionHistory;

public interface Consistency {
	public <T extends Jessy> boolean certify(T jessy, ExecutionHistory executionHistory);
}
