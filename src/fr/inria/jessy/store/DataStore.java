package fr.inria.jessy.store;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

import fr.inria.jessy.EntClass;

/**
 * @author Masoud Saeida Ardekani
 * 
 *         This class wraps the DPI of BerkeleyDB into a generic key-value API.
 * @param <T>
 */

public class DataStore {
	private Environment env;

	// Stores all EntityStores defined for this DataStore
	private Map<String, EntityStore> entityStores;

	/**
	 * Store all primary indexes of all entities manage by this DataStore Each
	 * entity class can have only one primary key. Thus, the key is the name of
	 * the entity class.
	 */
	private Map<String, PrimaryIndex<Long, ? extends JessyEntity>> primaryIndexes;

	/**
	 * Store all secondary indexes of all entities manage by this DataStore Each
	 * entity class can have multiple secondary keys. Thus, the key is the
	 * concatenation of entity class name and secondarykey name.
	 */
	private Map<String, SecondaryIndex<?, ?, ? extends JessyEntity>> secondaryIndexes;

	public DataStore(File envHome, boolean readOnly, String storeName) {
		setupEnvironment(envHome, readOnly);
		setupStore(readOnly, storeName);

		primaryIndexes = new HashMap<String, PrimaryIndex<Long, ? extends JessyEntity>>();
		secondaryIndexes = new HashMap<String, SecondaryIndex<?, ?, ? extends JessyEntity>>();
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

	private void setupStore(boolean readonly, String storeName) {
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		EntityStore store = new EntityStore(env, storeName, storeConfig);

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

	public <E extends JessyEntity> void addPrimaryIndex(String storeName,
			Class<E> entityClass) {
		//TODO Error Handling
		PrimaryIndex<Long, E> pindex = entityStores.get(storeName)
				.getPrimaryIndex(Long.class, entityClass);
		primaryIndexes.put(entityClass.getName(), pindex);
	}

	public <E extends JessyEntity, SK> void addSecondaryIndex(String storeName,
			Class<E> entityClass, Class<SK> secondaryKeyClass,
			String secondaryKeyName) {
		//TODO Commenting
		//TODO Error Handling
		PrimaryIndex<Long, ? extends JessyEntity> pindex = primaryIndexes
				.get(entityClass.getName());

		EntityStore store = entityStores.get(storeName);

		SecondaryIndex<SK, Long, ? extends JessyEntity> sindex = store
				.getSecondaryIndex(pindex, secondaryKeyClass, secondaryKeyName);
		secondaryIndexes.put(entityClass.getName()
				+ secondaryKeyName, sindex);
	}

	public <E extends JessyEntity> void put(E entity) {
		//TODO Commenting
		//TODO Error Handling
		@SuppressWarnings("unchecked")
		PrimaryIndex<Long, E> pindex = (PrimaryIndex<Long, E>) primaryIndexes
				.get(entity.getClass().getName());
		pindex.put(entity);
	}

	public <E extends JessyEntity, SK, V> E get(String storeName,
			Class<E> entityClass, String secondaryKeyName, SK keyValue, ArrayList<V> vectors) {

		SecondaryIndex<SK, Long, E> sindex =(SecondaryIndex<SK, Long, E>) secondaryIndexes
		.get(entityClass.getName() + secondaryKeyName);
		

		EntityCursor<E> cur = sindex.subIndex(keyValue).entities();
		//TODO Return according to VV rules!
		return cur.first();
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
