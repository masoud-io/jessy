package fr.inria.jessy.consistency;

import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import fr.inria.jessy.transaction.ExecutionHistory;

//TODO comment me
public class ConsistencyFactory {

	private static String ConsistencyType = readConfig();
	
	public static Consistency getConsistency() {
		if (ConsistencyType.equals("nmsi")) {
			return new NonMonotonicSnapshotIsolation();
		}
		return null;
	}
	
	public static Set<String> getConcerningKeys(ExecutionHistory executionHistory) {
		Set<String> keys=new HashSet<String>();
		if (ConsistencyType.equals("nmsi")) {
			keys.addAll(executionHistory.getWriteSet().getKeys());
			return keys;
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
