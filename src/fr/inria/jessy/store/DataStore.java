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

/**
 * @author Masoud Saeida Ardekani
 * 
 *         This class wraps the DPI of BerkeleyDB into a generic key-value API.
 * @param <T>
 */

public class DataStore {
	private Environment env;

	/**
	 * Indicates the default store that put and get operations should be
	 * executed in.
	 */
	private String defaultStore;

	/**
	 * Stores all EntityStores defined for this DataStore
	 */
	private Map<String, EntityStore> entityStores;

	/**
	 * Store all primary indexes of all entities manage by this DataStore Each
	 * entity class can have only one primary key. Thus, the key is the name of
	 * the entity class.
	 */
	private Map<String, PrimaryIndex<Long, ? extends JessyEntity>> primaryIndexes;

	/**
	 * Store all secondary indexes of all entities manage by this DataStore.
	 * Each entity class can have multiple secondary keys. Thus, the key is the
	 * concatenation of entity class name and secondarykey name.
	 */
	private Map<String, SecondaryIndex<?, ?, ? extends JessyEntity>> secondaryIndexes;

	public DataStore(File envHome, boolean readOnly, String storeName)
			throws Exception {
		entityStores = new HashMap<String, EntityStore>();
		primaryIndexes = new HashMap<String, PrimaryIndex<Long, ? extends JessyEntity>>();
		secondaryIndexes = new HashMap<String, SecondaryIndex<?, ?, ? extends JessyEntity>>();

		setupEnvironment(envHome, readOnly);
		addStore(readOnly, storeName);
		defaultStore = storeName;
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
		
		envConfig.setTransactional(false);
		

		//TODO database should be clean manually. EFFECT THE PERFORMANCE SUBSTANTIALLY
		envConfig.setLocking(false); //The cleaner becomes disable here! Influence the performance tremendously!

		envConfig.setSharedCache(true); //Does not effect the prformance much!

		//TODO subject to change for optimization
		envConfig.setCachePercent(90); 
		
		env = new Environment(envHome, envConfig);
	}

	/**
	 * Add a new store in BerkeleyDB. One store is automatically created when a
	 * datastore object is initialised.
	 * 
	 * @param readonly
	 *            true if the store is only for performing read operations.
	 * @param storeName
	 *            a unique store name.
	 */
	public void addStore(boolean readonly, String storeName) throws Exception {
		if (!entityStores.containsKey(storeName)) {
			StoreConfig storeConfig = new StoreConfig();
			storeConfig.setAllowCreate(true);
			
			//Caution: Durability cannot be ensured!
			storeConfig.setDeferredWrite(true);
			
			EntityStore store = new EntityStore(env, storeName, storeConfig);

			entityStores.put(storeName, store);
		} else {
			throw new Exception("Store already exists");
		}
	}

	public void close() {
		// TODO close stores!
		if (env != null) {
			try {
				env.close();
			} catch (DatabaseException ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * Create a primary index for an entity class that extends JessyEntity
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param storeName
	 *            the name of the store that the primary index works in. The
	 *            primary index stores entities inside this store.
	 * @param entityClass
	 *            A class that extends JessyEntity
	 */
	public <E extends JessyEntity> void addPrimaryIndex(String storeName,
			Class<E> entityClass) throws Exception {
		try {
			PrimaryIndex<Long, E> pindex = entityStores.get(storeName)
					.getPrimaryIndex(Long.class, entityClass);
			primaryIndexes.put(entityClass.getName(), pindex);
		} catch (NullPointerException ex) {
			throw new NullPointerException("Store with the name " + storeName
					+ " does not exists.");
		}
	}

	public <E extends JessyEntity> void addPrimaryIndex(Class<E> entityClass)
			throws Exception {
		addPrimaryIndex(defaultStore, entityClass);
	}

	/**
	 * Create a secondary index for an entity class that extends JessyEntity
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 *            the type of the secondary key field (annotated with
	 *            @SecondaryIndex)
	 * @param storeName
	 *            the name of the store that the primary index works in. The
	 *            primary index stores entities inside this store.
	 * @param entityClass
	 *            the class that extends JessyEntity
	 * @param secondaryKeyClass
	 *            Class of the secondary key field (annotated with
	 *            @SecondaryIndex)
	 * @param secondaryKeyName
	 *            Name of the secondary key field (annotated with
	 *            @SecondaryIndex)
	 */
	public <E extends JessyEntity, SK> void addSecondaryIndex(String storeName,
			Class<E> entityClass, Class<SK> secondaryKeyClass,
			String secondaryKeyName) throws Exception {
		try {
			PrimaryIndex<Long, ? extends JessyEntity> pindex = primaryIndexes
					.get(entityClass.getName());

			EntityStore store = entityStores.get(storeName);

			SecondaryIndex<SK, Long, ? extends JessyEntity> sindex = store
					.getSecondaryIndex(pindex, secondaryKeyClass,
							secondaryKeyName);
			secondaryIndexes.put(entityClass.getName() + secondaryKeyName,
					sindex);
		} catch (Exception ex) {
			throw new Exception("StoreName or PrimaryIndex does not exists");
		}
	}

	public <E extends JessyEntity, SK> void addSecondaryIndex(
			Class<E> entityClass, Class<SK> secondaryKeyClass,
			String secondaryKeyName) throws Exception {
		addSecondaryIndex(defaultStore, entityClass, secondaryKeyClass,
				secondaryKeyName);
	}

	/**
	 * Put the entity in the store using the primary key. Always adds a new
	 * entry
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param entity
	 *            entity to put inside the store
	 */
	public <E extends JessyEntity> void put(E entity)
			throws NullPointerException {
		try {
			@SuppressWarnings("unchecked")
			PrimaryIndex<Long, E> pindex = (PrimaryIndex<Long, E>) primaryIndexes
					.get(entity.getClass().getName());
			pindex.put(entity);
		} catch (NullPointerException ex) {
			throw new NullPointerException("PrimaryIndex cannot be found");
		}
	}

	/**
	 * Get the value of an entity object previously put.
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 *            the type of the secondary key field (annotated with
	 *            @SecondaryIndex)
	 * @param <V>
	 * @param entityClass
	 *            the class that extends JessyEntity
	 * @param secondaryKeyName
	 *            Name of the secondary key field (annotated with
	 *            @SecondaryIndex)
	 * @param keyValue the value of the secondary key.
	 * @param vectors
	 * @return
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity, SK, V> E get(Class<E> entityClass,
			String secondaryKeyName, SK keyValue, ArrayList<V> vectors)
			throws NullPointerException {
		try {
			@SuppressWarnings("unchecked")
			SecondaryIndex<SK, Long, E> sindex = (SecondaryIndex<SK, Long, E>) secondaryIndexes
					.get(entityClass.getName() + secondaryKeyName);

			EntityCursor<E> cur = sindex.subIndex(keyValue).entities();

			// TODO Return according to VV rules!
			return cur.last();
		} catch (NullPointerException ex) {
			throw new NullPointerException("SecondaryIndex cannot be found");
		}
	}

	/**
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 * @param <V>
	 * @param entityClass
	 * @param secondaryKeyName
	 * @param keyValue
	 * @return
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity, SK, V> int getEntityCounts(
			Class<E> entityClass, String secondaryKeyName, SK keyValue)
			throws NullPointerException {
		try {
			@SuppressWarnings("unchecked")
			SecondaryIndex<SK, Long, E> sindex = (SecondaryIndex<SK, Long, E>) secondaryIndexes
					.get(entityClass.getName() + secondaryKeyName);

			EntityCursor<E> cur = sindex.subIndex(keyValue).entities();
			if (cur.iterator().hasNext())
				return cur.count();
			else
				return 0;
		} catch (NullPointerException ex) {
			throw new NullPointerException("SecondaryIndex cannot be found");
		}
	}

	/**
	 * @return the defaultStore
	 */
	public String getDefaultStore() {
		return defaultStore;
	}

	/**
	 * @param defaultStore
	 *            the defaultStore to set
	 */
	public void setDefaultStore(String defaultStore) {
		this.defaultStore = defaultStore;
	}

	/**
	 * @return the entityStores
	 */
	public Map<String, EntityStore> getEntityStores() {
		return entityStores;
	}

}
