package fr.inria.jessy.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sourceforge.fractal.utils.PerformanceProbe.SimpleCounter;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.apache.log4j.Logger;

import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockMode;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityJoin;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.ForwardCursor;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

import fr.inria.jessy.store.JessyEntity.*;
import fr.inria.jessy.utils.Compress;
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

	protected static SimpleCounter failedReads = new SimpleCounter(
			"DataStore#failedReads");

	private EntityStore entityStore;

	/**
	 * Store all primary indexes of all entities manage by this DataStore Each
	 * entity class can have only one primary key. Thus, the key of the map is
	 * the name of the entity class.
	 */
	private Map<String, PrimaryIndex<PrimaryKeyType, ? extends JessyEntity>> primaryIndexes;

	/**
	 * Store all secondary indexes of all entities manage by this DataStore.
	 * Each entity class can have multiple secondary keys. Thus, the key of the
	 * map is the concatenation of entity class name and secondary key name.
	 */
	private Map<String, SecondaryIndex<?, ?, ? extends JessyEntity>> secondaryIndexes;

	public DataStore(File envHome, boolean readOnly, String storeName)
			throws Exception {
		primaryIndexes = new HashMap<String, PrimaryIndex<PrimaryKeyType, ? extends JessyEntity>>();
		secondaryIndexes = new HashMap<String, SecondaryIndex<?, ?, ? extends JessyEntity>>();

		setupEnvironment(envHome, readOnly);
		setupStore(readOnly, storeName);
	}

	/**
	 * Configure and Setup a berkeleyDB instance.
	 * 
	 * @param envHome
	 *            database home directory
	 * @param readOnly
	 *            whether the database should be opened as read-only or not
	 */
	private void setupEnvironment(File envHome, boolean readOnly) {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setInitializeCDB(true);
		envConfig.setAllowCreate(!readOnly);
//		envConfig.setInitializeCache(true);
//		envConfig.setReadOnly(readOnly);

		//		envConfig.setTransactional(false);
		// envConfig.setTxnNoSyncVoid(true);
		// envConfig.setTxnWriteNoSyncVoid(true);

		// TODO database should be clean manually. EFFECT THE PERFORMANCE
		// SUBSTANTIALLY
		// envConfig = envConfig.setLocking(false); // The cleaner becomes
		// disable
		// here!
		// Influence the performance tremendously!
		// envConfig.setSharedCache(true); // Does not effect the performance
		// much!
		// TODO subject to change for optimization
//		 envConfig.setCachePercent(90);
		envConfig.setInitializeCache(true);
		envConfig.setCacheSize(524288000);

		try {
			env = new Environment(envHome, envConfig);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Add a new store in BerkeleyDB. One store is automatically created when a
	 * data store object is initialized.
	 * 
	 * @param readonly
	 *            true if the store is only for performing read operations.
	 * @param storeName
	 *            a unique store name.
	 */
	public void setupStore(boolean readonly, String storeName) throws Exception {
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);

		// Caution: Durability cannot be ensured!
		// storeConfig.setDeferredWriteVoid(true);

		// storeConfig.setTransactionalVoid(false);

		entityStore = new EntityStore(env, storeName, storeConfig);
	}

	public synchronized void close() throws DatabaseException {
		if (env != null) {
			entityStore.close();
			// env.cleanLog();
			env.close();
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
	public <E extends JessyEntity> void addPrimaryIndex(Class<E> entityClass)
			throws Exception {
		PrimaryIndex<PrimaryKeyType, E> pindex = entityStore.getPrimaryIndex(
				PrimaryKeyType.class, entityClass);

		// PreloadConfig preloadConfig = new PreloadConfig();
		// preloadConfig.setMaxBytes(1073741824);
		// preloadConfig.setLoadLNs(true);

		primaryIndexes.put(Compress.compressClassName(entityClass.getName()),
				pindex);
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
	 * @param entityClassName
	 *            the class name of the entity
	 * @param secondaryKeyClass
	 *            Class of the secondary key field (annotated with
	 * @SecondaryIndex)
	 * @param secondaryKeyName
	 *            Name of the secondary key field (annotated with
	 * @SecondaryIndex)
	 */
	public <E extends JessyEntity, SK> void addSecondaryIndex(
			Class<E> entityClass, Class<SK> secondaryKeyClass,
			String secondaryKeyName) throws Exception {

		try {
			PrimaryIndex<PrimaryKeyType, ? extends JessyEntity> pindex = primaryIndexes
					.get(Compress.compressClassName(entityClass.getName()));

			SecondaryIndex<SK, PrimaryKeyType, ? extends JessyEntity> sindex = entityStore
					.getSecondaryIndex(pindex, secondaryKeyClass,
							secondaryKeyName);

			secondaryIndexes.put(
					Compress.compressClassName(entityClass.getName())
							+ secondaryKeyName, sindex);
		} catch (Exception ex) {
			throw new Exception(
					"StoreName or PrimaryIndex does not exists. Otherwise, entity field is not annottated properly.");
		}
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
	// FIXME need to GC old versions (there are some articles about this saying
	// that 5 is a good number).
	public <E extends JessyEntity> void put(E entity)
			throws NullPointerException {
		try {
			@SuppressWarnings("unchecked")
			PrimaryIndex<PrimaryKeyType, E> pindex = (PrimaryIndex<PrimaryKeyType, E>) primaryIndexes
					.get(Compress
							.compressClassName(entity.getClass().getName()));
			try {
				pindex.put(entity);
			} catch (DatabaseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (NullPointerException ex) {
			throw new NullPointerException("PrimaryIndex cannot be found");
		}
	}

	/**
	 * Get an entity object previously put inside data store. This entity object
	 * should be {@link Vector#isCompatible(CompactVector)} with the readSet
	 * vector.
	 * <p>
	 * Note: This method only returns one entity. Thus, it only works for a
	 * secondaryKey that is considered unique key in the application. Of course,
	 * they are not unique key inside BerkeleyDB because of storing different
	 * versions with different {@link Vector}.
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 *            the type of the secondary key field (annotated with
	 * @SecondaryKey)
	 * @param <V>
	 * @param entityClassName
	 *            the class name of the entity
	 * @param secondaryKeyName
	 *            Name of the secondary key field (annotated with
	 * @SecondaryKey)
	 * @param keyValue
	 *            the value of the secondary key.
	 * @param readSet
	 *            a compact vector that compactly contains versions of all
	 *            previously read entities.
	 * @return
	 * @throws NullPointerException
	 */
	private <E extends JessyEntity, SK> E get(String entityClassName,
			String secondaryKeyName, SK keyValue, CompactVector<String> readSet)
			throws NullPointerException {

		try {
			@SuppressWarnings("unchecked")
			SecondaryIndex<SK, Long, E> sindex = (SecondaryIndex<SK, Long, E>) secondaryIndexes
					.get(entityClassName + secondaryKeyName);

			EntityCursor<E> cur = sindex.subIndex(keyValue).entities();

			E entity = cur.first();

			if (readSet == null) {
				cur.close();
				return entity;
			}

			while (entity != null) {
				if (entity.getLocalVector().isCompatible(readSet) == Vector.CompatibleResult.COMPATIBLE) {
					cur.close();
					return entity;
				} else {
					if (entity.getLocalVector().isCompatible(readSet) == Vector.CompatibleResult.NOT_COMPATIBLE_TRY_NEXT) {
						entity = cur.next();
					}
					// NEVER_COMPATIBLE
					else {
						cur.close();
						break;
					}
				}
			}

			cur.close();
			failedReads.incr();
			return null;
		} catch (NullPointerException ex) {
			throw new NullPointerException("SecondaryIndex cannot be found");
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			throw new NullPointerException("SecondaryIndex cannot be found");
		}
	}

	/**
	 * Performs a query on several secondary keys, and returns the result set as
	 * a collection. All entities inside the collection should be
	 * {@link Vector#isCompatible(CompactVector)} with the readSet vector.
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 *            the type of the secondary key field (annotated with
	 * @SecondaryKey)
	 * @param entityClassName
	 *            the class name of the entity
	 * @param keyNameToValues
	 *            Map the name of the secondary key field (annotated with
	 * @SecondaryKey) to the desired value for the query.
	 * @param readSet
	 *            a compact vector that compactly contains versions of all
	 *            previously read entities.
	 * @return A collection of entities that the values of their keys are equal
	 *         to {@code keyNameToValues}
	 * @throws NullPointerException
	 */
	@SuppressWarnings("unchecked")
	private <E extends JessyEntity, SK> Collection<E> get(
			String entityClassName, List<ReadRequestKey<?>> keys,
			CompactVector<String> readSet) throws NullPointerException {
		try {

			SecondaryIndex sindex;
			PrimaryIndex<PrimaryKeyType, E> pindex = (PrimaryIndex<PrimaryKeyType, E>) primaryIndexes
					.get(entityClassName);
			EntityJoin<PrimaryKeyType, E> entityJoin = new EntityJoin<PrimaryKeyType, E>(
					pindex);

			for (ReadRequestKey key : keys) {
				sindex = secondaryIndexes.get(entityClassName
						+ key.getKeyName());
				entityJoin.addCondition(sindex, key.getKeyValue());
			}

			Map<String, E> results = new HashMap<String, E>();
			ForwardCursor<E> cur = entityJoin.entities();

			try {
				for (E entity : cur) {
					// FIXME Should the readSet be updated updated upon each
					// read?!
					if (entity.getLocalVector().isCompatible(readSet) == Vector.CompatibleResult.COMPATIBLE) {
						// Always writes the most recent committed version
						results.put(entity.getKey(), entity);
					}
				}
			} finally {
				cur.close();
			}

			return results.values();
		} catch (NullPointerException ex) {
			failedReads.incr();
			throw new NullPointerException("SecondaryIndex cannot be found");
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
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
	public <E extends JessyEntity, SK> ReadReply<E> get(
			ReadRequest<E> readRequest) throws NullPointerException {

		if (readRequest.isOneKeyRequest) {
			ReadRequestKey readRequestKey = readRequest.getOneKey();
			E entity = get(readRequest.getEntityClassName(),
					readRequestKey.getKeyName(), readRequestKey.getKeyValue(),
					readRequest.getReadSet());
			return new ReadReply<E>(entity, readRequest.getReadRequestId());

		} else {
			Collection<E> result = get(readRequest.getEntityClassName(),
					readRequest.getMultiKeys(), readRequest.getReadSet());

			return new ReadReply<E>(result, readRequest.getReadRequestId());
		}

	}

	@SuppressWarnings("unchecked")
	public <SK> List<ReadReply<JessyEntity>> getAll(
			List<ReadRequest<JessyEntity>> readRequests)
			throws NullPointerException {

		if (readRequests.isEmpty())
			return java.util.Collections.EMPTY_LIST;

		List<ReadReply<JessyEntity>> ret = new ArrayList<ReadReply<JessyEntity>>(
				readRequests.size());
		EntityCursor<JessyEntity> cur = null;
		String kindex = "";
		SecondaryIndex<SK, Long, JessyEntity> sindex;

		try {

			for (ReadRequest<JessyEntity> rr : readRequests) {

				if (!rr.isOneKeyRequest) {
					ret.add(new ReadReply<JessyEntity>(get(
							rr.getEntityClassName(), rr.getMultiKeys(),
							rr.getReadSet()), rr.getReadRequestId()));
				} else {

					ReadRequestKey rk = rr.getOneKey();
					kindex = rr.getEntityClassName() + rk.getKeyName();
					sindex = (SecondaryIndex<SK, Long, JessyEntity>) secondaryIndexes
							.get(kindex);
					CursorConfig cconfig=new CursorConfig();
					cconfig.setReadUncommitted(true);
					cur = sindex.subIndex((SK) rk.getKeyValue()).entities(null,cconfig);

					JessyEntity entity = cur.first(LockMode.READ_UNCOMMITTED);

					if (rr.getReadSet() == null) {
						ret.add(new ReadReply<JessyEntity>(
								(JessyEntity) entity, rr.getReadRequestId()));
						cur.close();
						continue;
					}

					while (entity != null) {
						if (entity.getLocalVector().isCompatible(
								rr.getReadSet()) == Vector.CompatibleResult.COMPATIBLE) {
							ret.add(new ReadReply<JessyEntity>(entity, rr
									.getReadRequestId()));
							break;
						} else if (entity.getLocalVector().isCompatible(
								rr.getReadSet()) == Vector.CompatibleResult.NEVER_COMPATIBLE) {
							failedReads.incr();
							entity = null;
						} else {
							entity = cur.next(LockMode.READ_UNCOMMITTED);
						}
					}

					if (entity == null)
						ret.add(new ReadReply<JessyEntity>((JessyEntity) null,
								rr.getReadRequestId()));

					cur.close();

				}
			}
		} catch (Exception ex) {

		}
		return ret;

	}

	/**
	 * Delete an entity with the provided secondary key from the berkeley DB.
	 * 
	 * @param <E>
	 *            the type that extends JessyEntity
	 * @param <SK>
	 *            the type of the secondary key field (annotated with
	 * @param entityClassName
	 *            the class name of the entity to be deleted
	 * @param secondaryKeyName
	 *            Name of the secondary key field (annotated with the value of
	 *            the secondary key.
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity, SK> boolean delete(String entityClassName,
			String secondaryKeyName, SK keyValue) throws NullPointerException {
		try {
			@SuppressWarnings("unchecked")
			String compressedName = Compress.compressClassName(entityClassName);

			SecondaryIndex<SK, Long, E> sindex = (SecondaryIndex<SK, Long, E>) secondaryIndexes
					.get(compressedName + secondaryKeyName);

			return sindex.delete(keyValue);
		} catch (NullPointerException ex) {
			throw new NullPointerException("SecondaryIndex cannot be found");
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
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
	 * @param entityClassName
	 *            the class name of the entity
	 * @param secondaryKeyName
	 * @param keyValue
	 * @return
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity, SK, V> int getEntityCounts(
			String entityClassName, String secondaryKeyName, SK keyValue)
			throws NullPointerException {
		try {
			@SuppressWarnings("unchecked")
			SecondaryIndex<SK, Long, E> sindex = (SecondaryIndex<SK, Long, E>) secondaryIndexes
					.get(entityClassName + secondaryKeyName);

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
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			throw new NullPointerException("SecondaryIndex cannot be found");
		}
	}

}
