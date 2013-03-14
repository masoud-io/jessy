package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.utils.CollectionUtils;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionTouchedKeys;

public abstract class NonMonotonicSnapshotIsolation extends Consistency {
	
	protected static Logger logger = Logger
			.getLogger(NonMonotonicSnapshotIsolation.class);

	static{
		READ_KEYS_REQUIRED_FOR_COMMUTATIVITY_TEST=false;
	}
	
	public NonMonotonicSnapshotIsolation(JessyGroupManager m, DataStore dataStore) {
		super(m, dataStore);
	}

	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {
		return !CollectionUtils.isIntersectingWith(history1.getWriteSet()
				.getKeys(), history2.getWriteSet().getKeys());
		
	}
	
	@Override
	public boolean certificationCommute(TransactionTouchedKeys tk1,
			TransactionTouchedKeys tk2) {
		return !CollectionUtils.isIntersectingWith(tk1.writeKeys, tk2.writeKeys);
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
