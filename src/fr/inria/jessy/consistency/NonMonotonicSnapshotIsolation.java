package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.utils.CollectionUtils;

import org.apache.log4j.Logger;

import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;

public abstract class NonMonotonicSnapshotIsolation extends Consistency {

	protected static Logger logger = Logger
			.getLogger(NonMonotonicSnapshotIsolation.class);

	public NonMonotonicSnapshotIsolation(DataStore dataStore) {
		super(dataStore);
	}

	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {

		return !CollectionUtils.isIntersectingWith(history1.getWriteSet()
				.getKeys(), history2.getWriteSet().getKeys());
		
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory,
			ConcernedKeysTarget target) {
		Set<String> keys = new HashSet<String>();
		keys.addAll(executionHistory.getWriteSet().getKeys());
		keys.addAll(executionHistory.getCreateSet().getKeys());
		return keys;
	}

}
