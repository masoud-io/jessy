package fr.inria.jessy.vector;

import java.io.FileInputStream;
import java.util.Properties;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;

public class VectorFactory {

	private static String consType = readConfig();

	public static <K> Vector<K> getVector(K selfKey) {
		if (consType.equals("nmsi")) {
			return new DependenceVector<K>(selfKey);
		}
		if (consType.equals("rc")) {
			return new NullVector<K>(selfKey);
		}
		if (consType.equals("ser")) {
			return new LightScalarVector<K>(selfKey);
		}
		if (consType.equals("si")) {
			return new ScalarVector<K>();
		}
		if (consType.equals("psi")) {
			return new VersionVector(JessyGroupManager.getInstance()
					.getMyGroup().name(), 0);
		}
		if (consType.equals("us")) {
			return new DependenceVector<K>(selfKey);
		}
		return null;
	}

	private static String readConfig() {
		String vectorType = "";
		try {
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					ConstantPool.CONFIG_PROPERTY);
			myProps.load(MyInputStream);
			vectorType = myProps.getProperty(ConstantPool.CONSISTENCY_TYPE);
			MyInputStream.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return vectorType;

	}

}
