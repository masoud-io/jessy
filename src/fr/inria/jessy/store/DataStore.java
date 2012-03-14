package fr.inria.jessy.store;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.Vector;

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
	 * entity class can have only one primary key. Thus, the key of the map is
	 * the name of the entity class.
	 */
	private Map<String, PrimaryIndex<Long, ? extends JessyEntity>> primaryIndexes;

	/**
	 * Store all secondary indexes of all entities manage by this DataStore.
	 * Each entity class can have multiple secondary keys. Thus, the key of the
	 * map is the concatenation of entity class name and secondarykey name.
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

		// TODO database should be clean manually. EFFECT THE PERFORMANCE
		// SUBSTANTIALLY
		// envConfig.setLocking(false); //The cleaner becomes disable here!
		// Influence the performance tremendously!

		envConfig.setSharedCache(true); // Does not effect the prformance much!

		// TODO subject to change for optimization
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

			// Caution: Durability cannot be ensured!
			// storeConfig.setDeferredWrite(true);

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
	 * @SecondaryIndex)
	 * @param storeName
	 *            the name of the store that the primary index works in. The
	 *            primary index stores entities inside this store.
	 * @param entityClass
	 *            the class that extends JessyEntity
	 * @param secondaryKeyClass
	 *            Class of the secondary key field (annotated with
	 * @SecondaryIndex)
	 * @param secondaryKeyName
	 *            Name of the secondary key field (annotated with
	 * @SecondaryIndex)
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
	 * Get an entity object previously put inside data store. This entity object
	 * should be {@link Vector#isCompatible(CompactVector)} with the readSet
	 * vector.
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 *            the type of the secondary key field (annotated with
	 * @SecondaryIndex)
	 * @param <V>
	 * @param entityClass
	 *            the class that extends JessyEntity
	 * @param secondaryKeyName
	 *            Name of the secondary key field (annotated with
	 * @SecondaryIndex)
	 * @param keyValue
	 *            the value of the secondary key.
	 * @param readSet
	 *            a compact vector that compactly contains versions of all
	 *            previously read entities.
	 * @return
	 * @throws NullPointerException
	 */
	private <E extends JessyEntity, SK> E get(Class<E> entityClass,
			String secondaryKeyName, SK keyValue, CompactVector<String> readSet)
			throws NullPointerException {
		try {
			@SuppressWarnings("unchecked")
			SecondaryIndex<SK, Long, E> sindex = (SecondaryIndex<SK, Long, E>) secondaryIndexes
					.get(entityClass.getName() + secondaryKeyName);

			EntityCursor<E> cur = sindex.subIndex(keyValue).entities();
			E entity = cur.last();

			if (readSet == null) {
				return entity;
			}

			while (entity != null) {
				if (entity.getLocalVector().isCompatible(readSet)) {
					cur.close();
					return entity;
				} else {
					entity = cur.prev();
				}
			}
			cur.close();
			return null;
		} catch (NullPointerException ex) {
			throw new NullPointerException("SecondaryIndex cannot be found");
		}
	}

	/**
	 * Get the value of an entity object previously put.
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 *            the type of the secondary key field (annotated with
	 * @param readRequest
	 * @return
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity, SK> ReadReply<E> get(ReadRequest<E, SK> readRequest)
			throws NullPointerException {
		E entity= get(readRequest.getEntityClass(), readRequest.getKeyName(),
				readRequest.getKeyvalue(),readRequest.getReadSet());
		
		return new ReadReply<E>(entity, readRequest.getReadRequestId());
	}


	/**
	 * Delete an entity with the provided secondary key from the berkeyley DB.
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 *            the type of the secondary key field (annotated with
	 * @param entityClass
	 *            the class that extends JessyEntity
	 * @param secondaryKeyName
	 *            Name of the secondary key field (annotated with the value of
	 *            the secondary key.
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity, SK> boolean delete(Class<E> entityClass,
			String secondaryKeyName, SK keyValue) throws NullPointerException {
		try {
			@SuppressWarnings("unchecked")
			SecondaryIndex<SK, Long, E> sindex = (SecondaryIndex<SK, Long, E>) secondaryIndexes
					.get(entityClass.getName() + secondaryKeyName);

			return sindex.delete(keyValue);
		} catch (NullPointerException ex) {
			throw new NullPointerException("SecondaryIndex cannot be found");
		}
	}

	/**
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 *            the type of the secondary key field (annotated with
	 * @SecondaryIndex)
	 * @param <V>
	 * @param entityClass
	 *            the class that extends JessyEntity
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
			if (cur.iterator().hasNext()) {
				int result = cur.count();
				cur.close();
				return result;
			} else {
				cur.close();
				return 0;
			}
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
