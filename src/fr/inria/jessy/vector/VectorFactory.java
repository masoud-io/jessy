package fr.inria.jessy.vector;

import java.io.FileInputStream;
import java.util.Properties;

public class VectorFactory {

	private static String vectorType = readConfig();

	public static <K> Vector<K> getVector(K selfKey) {
		if (vectorType == "dependencevector") {
			return new DependenceVector<K>(selfKey);
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
			vectorType = myProps.getProperty("vector_type");
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return vectorType;

	}
}
