package fr.inria.jessy.store;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

/**
 * @author Masoud Saeida Ardekani
 * 
 * 
 */

public class DataStore {
	private Environment env;
	private Map<String, EntityStore> entityStores;

	private Map<String, EntityStore> entityStores2;

	public DataStore(File envHome, boolean readOnly) {
		entityStores = new HashMap<String, EntityStore>();
		setupEnvironment(envHome, readOnly);
	}

	/**
	 * Configure and Setup a berkeleyDB instance.
	 * 
	 * @param envHome
	 *            database home directory
	 * @param readOnly
	 *            whether the database should be opened as readonly or not
	 */
	private void setupEnvironment(File envHome, boolean readOnly) {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setReadOnly(readOnly);
		envConfig.setAllowCreate(!readOnly);

		env = new Environment(envHome, envConfig);
	}

	public void close() {
		if (env != null) {
			try {
				env.close();
			} catch (DatabaseException ex) {
				ex.printStackTrace();
			}
		}
	}

	public boolean setupPrimaryIndexes(String entityStoreName,
			boolean readOnly, Map<Class, Class> entities) {

		if (!entityStores.containsKey(entityStoreName)) {
			StoreConfig storeConfig = new StoreConfig();
			storeConfig.setAllowCreate(readOnly);
			EntityStore store = new EntityStore(env, entityStoreName,
					storeConfig);

			entityStores.put(entityStoreName, store);

			for (Class entityClass : entities.keySet()) {
				Class primaryIndexClass = entities.get(entityClass);
				PrimaryIndex<primaryIndexClass, entityClass> indx = new store.getPrimaryIndex(
						primaryIndexClass, entityClass);
			}

			return true;
		}
		return false;

	}

}
