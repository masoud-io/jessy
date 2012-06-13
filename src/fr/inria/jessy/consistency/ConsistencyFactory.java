package fr.inria.jessy.consistency;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.utils.Configuration;

public class ConsistencyFactory {

	private static Logger logger = Logger.getLogger(ConsistencyFactory.class);

	private static String consistencyType;

	static {
		consistencyType = Configuration
				.readConfig(ConstantPool.CONSISTENCY_TYPE);
		logger.warn("Consistency is " + consistencyType);
	}

	public static Consistency getConsistency(DataStore dataStore) {

		if (consistencyType.equals("nmsi")) {
			return new NonMonotonicSnapshotIsolation(dataStore);
		} else if (consistencyType.equals("si")) {
			return new SnapshotIsolation(dataStore);
		} else if (consistencyType.equals("ser")) {
			return new Serializability(dataStore);
		} else if (consistencyType.equals("rc")) {
			return new ReadComitted(dataStore);
		} else if (consistencyType.equals("psi")) {
			return new ParallelSnapshotIsalation(dataStore);
		}
		return null;
	}

}
