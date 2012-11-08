package fr.inria.jessy.store;

import java.io.File;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.utils.Configuration;

public class DataStoreFactory {

	private static Logger logger = Logger.getLogger(DataStoreFactory.class);

	private static DataStore _instance;

	private static String dataStoreType;

	static {
		dataStoreType = Configuration.readConfig(ConstantPool.DATA_STORE_TYPE);
		logger.info("Datastore is : " + dataStoreType);
	}

	public static DataStore getDataStoreInstance() {
		if (_instance != null)
			return _instance;

		try {
			if (dataStoreType.equals("hashmap")) {
				_instance = new HashMapDataStore();
			} else {
				File environmentHome = new File(System.getProperty("user.dir"));
				boolean readOnly = false;
				String storeName = "store";
				_instance = new BerkeleyDBDataStore(environmentHome, readOnly,
						storeName);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return _instance;
	}

}
