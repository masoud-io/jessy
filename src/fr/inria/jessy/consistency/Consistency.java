package fr.inria.jessy.consistency;

import java.util.concurrent.ConcurrentMap;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;

public interface Consistency {

	public boolean certify(
			ConcurrentMap<String, JessyEntity> lastCommittedEntities,
			ExecutionHistory executionHistory);

}
