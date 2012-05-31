package fr.inria.jessy.utils;

import java.io.FileInputStream;
import java.util.Properties;

public class Configuration {
	public static String readConfig(String propName) {
		try {
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					fr.inria.jessy.ConstantPool.CONFIG_PROPERTY);
			myProps.load(MyInputStream);
			return myProps.getProperty(propName);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return "";
	}
}
