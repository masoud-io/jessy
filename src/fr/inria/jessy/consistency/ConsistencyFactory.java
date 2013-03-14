package fr.inria.jessy.consistency;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
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

	public static Consistency initConsistency(JessyGroupManager m, DataStore dataStore) {
		if (_instance != null)
			return _instance;

		if (consistencyTypeName.equals("nmsi")) {
			_instance = new NonMonotonicSnapshotIsolationWithDependenceVector(m, dataStore);
		} else if (consistencyTypeName.equals("nmsi2")) {
			_instance = new NonMonotonicSnapshotIsolationWithGMUVector(m,
					dataStore);
		} else if (consistencyTypeName.equals("si")) {
			//TODO
			System.err.println("si with ab-cast is not yet implemented. Use si2 instead");
//			_instance = new SnapshotIsolationWithBroadcast(dataStore);
		} else if (consistencyTypeName.equals("si2")) {
			_instance = new SnapshotIsolation(m, dataStore);
		} else if (consistencyTypeName.equals("ser")) {
			_instance = new Serializability(m, dataStore);
		} else if (consistencyTypeName.equals("rc")) {
			_instance = new ReadComitted(m, dataStore);
		} else if (consistencyTypeName.equals("psi")) {
			_instance = new ParallelSnapshotIsalation(m, dataStore);
		} else if (consistencyTypeName.equals("us")) {
			_instance = new UpdateSerializabilityWithDependenceVector(m, dataStore);
		} else if (consistencyTypeName.equals("us2")) {
			_instance = new UpdateSerializabilityWithGMUVector(m, dataStore);
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
