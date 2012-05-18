package fr.inria.jessy.vector;

import java.io.FileInputStream;
import java.util.Properties;

public class VectorFactory {

	private static String vectorType = readConfig();

	public static <K> Vector<K> getVector(K selfKey) {
		if (vectorType.equals("dependencevector")) {
			return new DependenceVector<K>(selfKey);
		}
		if(vectorType.equals("nullvector")){
			return new NullVector<K>(selfKey);
		}
		if(vectorType.equals("lightscalarvector")){
			return new LightScalarVector<K>();
		}
		if(vectorType.equals("scalarvector")){
			return new ScalarVector<K>(selfKey);
		}
		
		return null;
	}
	
	public static void changeConfig(String t){
		vectorType=t;
	}
	
	private static String readConfig() {
		String vectorType = "";
		try {
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					"config.property");
			myProps.load(MyInputStream);
			vectorType = myProps.getProperty("vector_type");
			MyInputStream.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return vectorType;

	}
		
}
