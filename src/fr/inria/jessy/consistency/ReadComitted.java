package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;

public class ReadComitted extends Consistency {

	public ReadComitted(DataStore store) {
		super(store);
		Consistency.SEND_READSET_DURING_TERMINATION=false;
	}

	@Override
	public boolean certify(ExecutionHistory executionHistory) {
		return true;
	}

	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {
		return true;
	}
	
	@Override
	public boolean applyingTransactionCommute() {
		return true;
	}

	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {

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
