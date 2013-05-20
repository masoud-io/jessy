package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.utils.CollectionUtils;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionTouchedKeys;

public abstract class SI extends Consistency {
	private static Logger logger = Logger
			.getLogger(SI.class);

	static{
		READ_KEYS_REQUIRED_FOR_COMMUTATIVITY_TEST=false;
	}
	
	public SI(JessyGroupManager m, DataStore store) {
		super(m, store);
		Consistency.SEND_READSET_DURING_TERMINATION=false;
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory,
			ConcernedKeysTarget target) {

		Set<String> keys = new HashSet<String>(4);
		if (target == ConcernedKeysTarget.TERMINATION_CAST) {

			/*
			 * If it is a read-only transaction, we return an empty set. But if
			 * it is not an empty set, then we have to return a set that
			 * contains a key every group. We do this to simulate the atomic
			 * broadcast behavior. Because, later, this transaction will atomic
			 * multicast to all the groups.
			 */
			if (executionHistory.getWriteSet().size() == 0
					&& executionHistory.getCreateSet().size() == 0)
				return new HashSet<String>(0);

			keys=manager.getPartitioner().generateKeysInAllGroups();
			return keys;
		} else if (target == ConcernedKeysTarget.SEND_VOTES) {
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
			return keys;
		} else {
			keys=manager.getPartitioner().generateKeysInAllGroups();
			return keys;
		}
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
		
}