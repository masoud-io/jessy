package fr.inria.jessy.consistency;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.communication.NonGenuineTerminationCommunication;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.store.DataStore;

public class SnapshotIsolationWithBroadcast extends SnapshotIsolation {

	public SnapshotIsolationWithBroadcast(DataStore store){
		super(store);
	}
	
	@Override
	public TerminationCommunication getOrCreateTerminationCommunication(
			Group group, Learner learner) {
		if (terminationCommunication == null) {
			terminationCommunication = new NonGenuineTerminationCommunication(
					group, learner);

		}
		return terminationCommunication;
	}
	

}
