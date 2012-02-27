package fr.inria.jessy.consistency;

import java.io.FileInputStream;
import java.util.Properties;

public class ConsistencyFactory {

	private static String ConsistencyType = readConfig();
	
	public static Consistency getConsistency() {
		if (ConsistencyType== "nmsi") {
			return new NonMonotonicSnapshotIsolation();
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
