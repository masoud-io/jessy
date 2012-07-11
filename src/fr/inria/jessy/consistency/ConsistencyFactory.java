package fr.inria.jessy.consistency;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.utils.Configuration;

public class ConsistencyFactory {

	private static Logger logger = Logger.getLogger(ConsistencyFactory.class);

	private static Consistency _instance;

	private static String consistencyType;

	static {
		consistencyType = Configuration
				.readConfig(ConstantPool.CONSISTENCY_TYPE);
		logger.warn("Consistency is " + consistencyType);
	}

	public static Consistency initConsistency(DataStore dataStore) {
		if (_instance != null)
			return _instance;

		if (consistencyType.equals("nmsi")) {
			_instance = new NonMonotonicSnapshotIsolation(dataStore);
		} else if (consistencyType.equals("si")) {
			_instance = new SnapshotIsolation(dataStore);
		} else if (consistencyType.equals("si2")) {
			_instance = new SnapshotIsolationWithMulticast(dataStore);
		} else if (consistencyType.equals("ser")) {
			_instance = new Serializability(dataStore);
		} else if (consistencyType.equals("rc")) {
			_instance = new ReadComitted(dataStore);
		} else if (consistencyType.equals("psi")) {
			_instance = new ParallelSnapshotIsalation(dataStore);
		} else if (consistencyType.equals("us")) {
			_instance = new UpdateSerializability(dataStore);
		}
		return _instance;
	}

	public static Consistency getConsistencyInstance() {
		return _instance;
	}
}
