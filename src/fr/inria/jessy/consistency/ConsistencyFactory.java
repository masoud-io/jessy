package fr.inria.jessy.consistency;

import java.io.FileInputStream;
import java.util.Properties;

import fr.inria.jessy.store.DataStore;

//TODO comment me
public class ConsistencyFactory {

	private static String ConsistencyType = readConfig();

	public static Consistency getConsistency(DataStore dataStore) {

		if (ConsistencyType.equals("nmsi")) {
			return new NonMonotonicSnapshotIsolation(dataStore);
		} else if (ConsistencyType.equals("si")) {
			return new SnapshotIsolation(dataStore);
		} else if (ConsistencyType.equals("ser")) {
			return new Serializability(dataStore);
		}
		return null;
	}

	private static String readConfig() {
		try {
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					fr.inria.jessy.ConstantPool.CONFIG_PROPERTY);
			myProps.load(MyInputStream);
			return myProps
					.getProperty(fr.inria.jessy.ConstantPool.CONSISTENCY_TYPE);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return "";
	}

}
