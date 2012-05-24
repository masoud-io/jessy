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
		String vectorType = "";
		try {
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					"config.property");
			myProps.load(MyInputStream);
			vectorType = myProps.getProperty("consistency_type");
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return vectorType;

	}

}
