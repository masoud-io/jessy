package fr.inria.jessy.store;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

/**
 * @author Masoud Saeida Ardekani
 * 
 *         This class wraps the DPI of BerkeleyDB into a generic key-value API.
 */

public class DataStore {
	private Environment env;
	private Map<String, EntityStore> entityStores;

	public DataStore(File envHome, boolean readOnly, String storeName) {
		setupEnvironment(envHome, readOnly);
		setupStore(readOnly, storeName);
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

	private void setupStore(boolean readonly,String storeName){
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		EntityStore store= new EntityStore(env, storeName, storeConfig);

		entityStores = new HashMap<String, EntityStore>();
		entityStores.put(storeName, store);		
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

	/**
	 * @return the entityStores
	 */
	public Map<String, EntityStore> getEntityStores() {
		return entityStores;
	}

	
	
	/*
	 * public <PK, SK, E> void addEntityPrimaryIndex(String storeName, Class<PK>
	 * primaryKeyIndexClass, Class<SK> secondaryKeyIndexClass, String
	 * secondaryKeyName, Class<E> entityClass) {
	 * 
	 * EntityStore store; if (!entity2Stores.containsKey(storeName)) {
	 * StoreConfig storeConfig = new StoreConfig();
	 * storeConfig.setAllowCreate(true); store = new EntityStore(env, storeName,
	 * storeConfig); entity2Stores.put(storeName, store);
	 * 
	 * PrimaryIndex<PK, E> pindex= store.getPrimaryIndex(primaryKeyIndexClass,
	 * entityClass); entity2PrimaryIndexes.put(storeName, pindex);
	 * 
	 * SecondaryIndex<SK,PK, E> sindex=store.getSecondaryIndex(pindex,
	 * secondaryKeyIndexClass, secondaryKeyName);
	 * entity2SecondaryIndexes.put(storeName, sindex);
	 * 
	 * } else { // The store already exists.
	 * 
	 * }
	 */

}
