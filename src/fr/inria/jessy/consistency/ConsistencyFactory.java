package fr.inria.jessy.consistency;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.utils.Configuration;

public class ConsistencyFactory {

	private static Logger logger = Logger.getLogger(ConsistencyFactory.class);

	private static Consistency _instance;

	private static String consistencyTypeName;

	static {
		consistencyTypeName = Configuration
				.readConfig(ConstantPool.CONSISTENCY_TYPE);
		logger.warn("Consistency is " + consistencyTypeName);
	}

	public static Consistency initConsistency(DataStore dataStore) {
		if (_instance != null)
			return _instance;

		if (consistencyTypeName.equals("nmsi")) {
			_instance = new NonMonotonicSnapshotIsolationWithDependenceVector(dataStore);
		} else if (consistencyTypeName.equals("nmsi2")) {
			_instance = new NonMonotonicSnapshotIsolationWithGMUVector(
					dataStore);
		} else if (consistencyTypeName.equals("si")) {
			_instance = new SnapshotIsolationWithBroadcast(dataStore);
		} else if (consistencyTypeName.equals("si2")) {
			_instance = new SnapshotIsolationWithMulticast(dataStore);
		} else if (consistencyTypeName.equals("ser")) {
			_instance = new Serializability(dataStore);
		} else if (consistencyTypeName.equals("rc")) {
			_instance = new ReadComitted(dataStore);
		} else if (consistencyTypeName.equals("psi")) {
			_instance = new ParallelSnapshotIsalation(dataStore);
		} else if (consistencyTypeName.equals("us")) {
			_instance = new UpdateSerializabilityWithDependenceVector(dataStore);
		} else if (consistencyTypeName.equals("us2")) {
			_instance = new UpdateSerializabilityWithGMUVector(dataStore);
		}
		return _instance;
	}

	public static Consistency getConsistencyInstance() {
		return _instance;
	}

	public static String getConsistencyTypeName() {
		return consistencyTypeName;
	}
	
	
}
