package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionTouchedKeys;

public class ReadComitted extends Consistency {

	static{
		READ_KEYS_REQUIRED_FOR_COMMUTATIVITY_TEST=false;
	}
	
	public ReadComitted(JessyGroupManager m, DataStore store) {
		super(m, store);
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
	public boolean certificationCommute(TransactionTouchedKeys tk1,
			TransactionTouchedKeys tk2) {
		return true;
	}
	
	@Override
	public boolean applyingTransactionCommute() {
		return true;
	}

	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {

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
