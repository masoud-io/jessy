package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.communication.TrivialTerminationCommunication;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;

public class ReadComitted extends Consistency {

	public ReadComitted(DataStore store) {
		super(store);
	}

	@Override
	public TerminationCommunication getOrCreateTerminationCommunication(
			Group group, Learner learner) {
		if (terminationCommunication == null)
			/*
			 * Do not return {@code TrivialTerminationCommunication} instance
			 * because it may lead to <i>deadlock</i>.
			 */
			terminationCommunication = new TrivialTerminationCommunication(
					group, learner);
		return terminationCommunication;
	}

	@Override
	public boolean certify(ExecutionHistory executionHistory) {
		return true;
	}

	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {

		return checkCommutativity;
	}

	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {

	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory) {
		Set<String> keys = new HashSet<String>();
		keys.addAll(executionHistory.getWriteSet().getKeys());
		keys.addAll(executionHistory.getCreateSet().getKeys());
		return keys;
	}

}
