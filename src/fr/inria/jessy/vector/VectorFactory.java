package fr.inria.jessy.vector;

import java.io.FileInputStream;
import java.util.Properties;

import fr.inria.jessy.ConstantPool;


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
			return new LightScalarVector<K>(selfKey);
		}
		if(vectorType.equals("scalarvector")){
			return new ScalarVector<K>();
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
					ConstantPool.CONFIG_PROPERTY);
			myProps.load(MyInputStream);
			vectorType = myProps.getProperty(ConstantPool.VECTOR_TYPE);
			MyInputStream.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return vectorType;

	}
		
}
