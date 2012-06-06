package fr.inria.jessy.consistency;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.utils.Configuration;

public class ConsistencyFactory {

	private static String ConsistencyType = Configuration
			.readConfig(ConstantPool.CONSISTENCY_TYPE);

	public static Consistency getConsistency(DataStore dataStore) {

		if (ConsistencyType.equals("nmsi")) {
			return new NonMonotonicSnapshotIsolation(dataStore);
		} else if (ConsistencyType.equals("si")) {
			return new SnapshotIsolation(dataStore);
		} else if (ConsistencyType.equals("ser")) {
			return new Serializability(dataStore);
		} else if (ConsistencyType.equals("rc")) {
			return new ReadComitted(dataStore);
		}
		return null;
	}

}
