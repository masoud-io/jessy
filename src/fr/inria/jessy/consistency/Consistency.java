package fr.inria.jessy.consistency;

import java.util.concurrent.ConcurrentMap;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;

public interface Consistency {
	
	public <T extends JessyEntity> boolean certify(ConcurrentMap<String, T> lastCommittedEntities, ExecutionHistory executionHistory);
}
