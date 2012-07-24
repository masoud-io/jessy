package fr.inria.jessy.vector;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.utils.Configuration;

public class VectorFactory {

	private static String consType = Configuration.readConfig(ConstantPool.CONSISTENCY_TYPE);
	
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
		if (consType.equals("si") || consType.equals("si2")) {
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


}
