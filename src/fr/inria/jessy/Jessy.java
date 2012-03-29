package fr.inria.jessy;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;

import com.sleepycat.persist.model.SecondaryKey;

import fr.inria.jessy.consistency.Consistency;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionState;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.CompactVector;

/**
 * Jessy is the abstract base class for local and distributed Jessy
 * implementation.
 * 
 * @author Masoud Saeida Ardekani
 */
public abstract class Jessy {

	public enum ExecutionMode {
		/**
		 * Jessy only executes transactional operations.
		 */
		TRANSACTIONAL,
		/**
		 * Jessy only executes non-transactional operations.
		 */
		NON_TRANSACTIONAL,
		/**
		 * execution mode is not defined yet.
		 */
		UNDEFINED,
	};

	
	private DataStore dataStore;
	Consistency consistency;

	ConcurrentMap<TransactionHandler, ExecutionHistory> handler2executionHistory;
	private List<Class<? extends JessyEntity>> entityClasses;

	private CopyOnWriteArraySet<TransactionHandler> commitedTransactions;
	private CopyOnWriteArraySet<TransactionHandler> abortedTransactions;

	/**
	 * Stores the last committed entities in a concurrent map. This is used
	 * during certification to check for conflicting concurrent transactions.
	 * The key is the concatenation of entity class name and entity secondary
	 * key {@link JessyEntity#getKey()} . The value is the entity.
	 */
	ConcurrentMap<String, JessyEntity> lastCommittedEntities;

	private ExecutionMode transactionalAccess = ExecutionMode.UNDEFINED;

	protected Jessy() throws Exception {

		// TODO load from system property
		File environmentHome = new File(System.getProperty("user.dir"));
		boolean readOnly = false;
		String storeName = "store";

		dataStore = new DataStore(environmentHome, readOnly, storeName);
		consistency = ConsistencyFactory.getConsistency();

		handler2executionHistory = new ConcurrentHashMap<TransactionHandler, ExecutionHistory>();

		entityClasses = new ArrayList<Class<? extends JessyEntity>>();

		commitedTransactions = new CopyOnWriteArraySet<TransactionHandler>();
		abortedTransactions = new CopyOnWriteArraySet<TransactionHandler>();

		lastCommittedEntities = new ConcurrentHashMap<String, JessyEntity>();
		
	}

	protected DataStore getDataStore() {
		return dataStore;
	}

	protected ExecutionHistory getExecutionHistory(
			TransactionHandler transactionHandler) {
		return handler2executionHistory.get(transactionHandler);
	}

	public JessyEntity getLastCommittedEntity(String key) {
		return lastCommittedEntities.get(key);
	}

	/**
	 * Setup a primary index and secondary index in the data store for
	 * performing read and write operations on entities with type E.
	 * <p>
	 * This method only creates one secondary index for the default secondary
	 * key. In order to performs reads on different secondary keys (fields), and
	 * not on the default secondaryKey, first, an index for each of them should
	 * be created by calling {@link #addSecondaryIndex(Class, ArrayList)}
	 * 
	 * @param <E>
	 *            The Type of the entity that will be store/retrieve
	 * @param entityClass
	 *            The Class of the entity that will be store/retrieve
	 * @throws Exception
	 */
	public <E extends JessyEntity> void addEntity(Class<E> entityClass)
			throws Exception {
		dataStore.addPrimaryIndex(entityClass);
		dataStore.addSecondaryIndex(entityClass, String.class, "secondaryKey");
		entityClasses.add(entityClass);
	}

	/**
	 * Setup an additional secondary index in the data store for performing read
	 * operations on entities with type <code>E</code>, and on the field of type
	 * <code>SK</code>, named
	 * <code>keyName<code> and annotated with {@link SecondaryKey}
	 * 
	 * @param <E>
	 *            The Type of the entity that will be retrieve
	 * @param <SK>
	 *            The Type of the additional secondary key.
	 * @param entityClass
	 *            The Class of the entity that will be retrieve
	 * @param keyClass
	 *            The class of the additional secondary key
	 * @param keyName
	 *            The name of the additional secondary Key
	 * @throws Exception
	 */
	public <E extends JessyEntity, SK> void addSecondaryIndex(
			Class<E> entityClass, Class<SK> keyClass, String keyName)
			throws Exception {

		dataStore.addSecondaryIndex(entityClass, keyClass, keyName);
	}

	/**
	 * This method should be called for reading an entity with a query on a
	 * default secondary key.
	 * <p>
	 * Executes a read operation on Jessy. It first checks to see if the
	 * transaction has written a value for the same entity or not. If it has
	 * written a new value previously, it returns that value, otherwise, it
	 * calls the {@link Jessy#performRead(Class, String, Object, List)} method.
	 * <p>
	 * This read is performed on {@link JessyEntity#getSecondaryKey()}
	 * 
	 * @param <E>
	 *            Type of the entity to read the value from.
	 * @param entityClass
	 *            Class of the entity to read the value from.
	 * @param keyValue
	 *            The value of the secondary key
	 * @return The entity with the secondary key value equals keyValue
	 */
	public <E extends JessyEntity> E read(
			TransactionHandler transactionHandler, Class<E> entityClass,
			String keyValue) throws Exception {

		ExecutionHistory executionHistory = handler2executionHistory
				.get(transactionHandler);

		if (executionHistory == null) {
			throw new NullPointerException("Transaction has not been started");
		}

		E entity;
		entity = executionHistory.getWriteEntity(entityClass, keyValue);

		// we first check it this entity has been updated in this transaction
		// before!
		if (entity == null) {

			// if the entity has not been updated, we check if it has been read
			// in the same transaction before.
			entity = executionHistory.getReadEntity(entityClass, keyValue);

			if (entity == null)
				entity = performRead(entityClass, "secondaryKey", keyValue,
						executionHistory.getReadSet().getCompactVector());
		}

		if (entity != null) {
			executionHistory.addReadEntity(entity);
			return entity;
		} else {
			return null;
		}
	}

	/**
	 * TODO returned an already read entity from the local cache.
	 * 
	 * This method should be called for reading an entity with a query on
	 * keyName that is not a default secondary key. It returns only one entity,
	 * thus the key should be unique key.
	 * <p>
	 * Executes a read operation on Jessy. It calls the
	 * {@link Jessy#performRead(Class, String, Object, List)} method to read the
	 * data.
	 * <p>
	 * This read is performed on {@link JessyEntity#getSecondaryKey()}
	 * 
	 * @param <E>
	 *            The Type of the entity to read the value from.
	 * @param <SK>
	 *            The Type of the secondary key to read the value from.
	 * @param entityClass
	 *            The Class of the entity to read the value from.
	 * @param keyName
	 *            The name of the secondary key.
	 * @param keyValue
	 *            The value of the secondary key
	 * @return The entity with the keyName field value equals keyValue
	 */
	public <E extends JessyEntity, SK> E read(
			TransactionHandler transactionHandler, Class<E> entityClass,
			String keyName, SK keyValue) throws Exception {

		ExecutionHistory executionHistory = handler2executionHistory
				.get(transactionHandler);

		if (executionHistory == null) {
			throw new NullPointerException("Transaction has not been started");
		}

		E entity;

		/*
		 * If keyName is null, then it is a read operation on the secondaryKey.
		 * Otherwise, it is like a where clause. The query should be performed
		 * on another field of the entity.
		 */
		if (keyName == null)
			entity = performRead(entityClass, "secondaryKey", keyValue,
					executionHistory.getReadSet().getCompactVector());
		else
			entity = performRead(entityClass, keyName, keyValue,
					executionHistory.getReadSet().getCompactVector());

		if (entity != null) {
			executionHistory.addReadEntity(entity);
			return entity;
		} else {
			return null;
		}
	}

	/**
	 * 
	 * Executes a read operation ONLY on Jessy. It calls the
	 * {@link Jessy#performRead(Class, String, Object, List)} method to read the
	 * data.
	 * <p>
	 * This read is performed on all keys provided {@code keys} This method
	 * never checks local cache!!!!
	 * <p>
	 * If the cardinality is not known in advance, always use this method since
	 * it returns all consistent entities corresponding to the keys.
	 * 
	 * @param <E>
	 *            The Type of the entity to read the value from.
	 * @param <SK>
	 *            The Type of the secondary key to read the value from.
	 * @param entityClass
	 *            The Class of the entity to read the value from.
	 * @param keyName
	 *            The name of the secondary key.
	 * @param keyValue
	 *            The value of the secondary key
	 * @return The entity with the keyName field value equals keyValue
	 */
	public <E extends JessyEntity, SK> Collection<E> read(
			TransactionHandler transactionHandler, Class<E> entityClass,
			List<ReadRequestKey<?>> keys) throws Exception {

		ExecutionHistory executionHistory = handler2executionHistory
				.get(transactionHandler);

		if (executionHistory == null) {
			throw new NullPointerException("Transaction has not been started");
		}

		Collection<E> entities = performRead(entityClass, keys,
				executionHistory.getReadSet().getCompactVector());

		if (entities != null) {
			executionHistory.addReadEntity(entities);
			return entities;
		} else {
			return null;
		}
	}

	/**
	 * Performs a local or remote read operation depending on the specific
	 * implementation of Jessy.
	 * 
	 * @param <E>
	 *            Type of the entity to read the value from.
	 * @param <SK>
	 *            Type of the secondary key to read the value from.
	 * @param entityClass
	 *            Class of the entity to read the value from.
	 * @param keyName
	 *            The name of the secondary key
	 * @param keyValue
	 *            The value of the secondary key
	 * @param readList
	 *            List of vectors of already executed read operations.
	 * @return An entity with the secondary key equals keyName and its value
	 *         equals keyValue
	 */
	protected abstract <E extends JessyEntity, SK> E performRead(
			Class<E> entityClass, String keyName, SK keyValue,
			CompactVector<String> readSet) throws InterruptedException,
			ExecutionException;

	/**
	 * Performs a local or remote read operation depending on the specific
	 * implementation of Jessy on all provided keys.
	 * 
	 * @param <E>
	 *            Type of the entity to read the value from.
	 * @param <SK>
	 *            Type of the secondary key to read the value from.
	 * @param entityClass
	 *            Class of the entity to read the value from.
	 * @param keyName
	 *            The name of the secondary key
	 * @param keyValue
	 *            The value of the secondary key
	 * @param readList
	 *            List of vectors of already executed read operations.
	 * @return An entity with the secondary key equals keyName and its value
	 *         equals keyValue
	 */
	protected abstract <E extends JessyEntity> Collection<E> performRead(
			Class<E> entityClass, List<ReadRequestKey<?>> keys,
			CompactVector<String> readSet) throws InterruptedException,
			ExecutionException;

	/**
	 * Stores the entity locally. The locally stored entities will be stored in
	 * the database upon calling {@link Jessy#commitTransaction()}.
	 * 
	 * @param <E>
	 *            Type of the entity to read the value from.
	 * @param entity
	 */
	public <E extends JessyEntity> void write(
			TransactionHandler transactionHandler, E entity)
			throws NullPointerException {

		ExecutionHistory executionHistory = handler2executionHistory
				.get(transactionHandler);

		if (executionHistory == null) {
			throw new NullPointerException("Transaction has not been started");
		} else {

			// First checks if we have already read an entity with the same key!
			// TODO make this conditional according to user definition! (if
			// disabled, performance gain)
			JessyEntity tmp = executionHistory.getReadEntity(entity.getClass(),
					entity.getSecondaryKey());
			if (tmp == null) {
				// the opeation is a blind write! First issue a read operation.
				try {
					read(entity.getClass(), entity.getSecondaryKey());
				} catch (Exception e) {
					// if this is a first write opeation, then it comes here!
				}
			}
			executionHistory.addWriteEntity(entity);
		}
	}

	/**
	 * Add the entity into the createSet.
	 * <p>
	 * TODO It should be checked whether this entity has been put or not. If the
	 * above rule is ensured by the client, then create is much faster. (only
	 * one write)
	 */
	public <E extends JessyEntity> void create(
			TransactionHandler transactionHandler, E entity)
			throws NullPointerException {

		ExecutionHistory executionHistory = handler2executionHistory
				.get(transactionHandler);

		if (executionHistory == null) {
			throw new NullPointerException("Transaction has not been started");
		} else {
			executionHistory.addCreateEntity(entity);
		}
	}

	public <E extends JessyEntity> void remove(
			TransactionHandler transactionHandler, E entity)
			throws NullPointerException {
		entity.removoe();
		write(transactionHandler, entity);
	}

	public TransactionHandler startTransaction() throws Exception {
		if (transactionalAccess == ExecutionMode.UNDEFINED)
			transactionalAccess = ExecutionMode.TRANSACTIONAL;

		if (transactionalAccess == ExecutionMode.TRANSACTIONAL) {
			TransactionHandler transactionHandler = new TransactionHandler();
			ExecutionHistory executionHistory = new ExecutionHistory(
					entityClasses, transactionHandler);
			executionHistory.changeState(TransactionState.EXECUTING);
			handler2executionHistory.put(transactionHandler, executionHistory);
			return transactionHandler;

		}
		throw new Exception(
				"Jessy has been accessed in non-transactional way. It cannot be accesesed transactionally");
	}

	/**
	 * Commit the open transaction, and garbage collect it.
	 * 
	 * @return
	 */
	public ExecutionHistory commitTransaction(
			TransactionHandler transactionHandler) {
		ExecutionHistory result = handler2executionHistory
				.get(transactionHandler);

		result.changeState(TransactionState.COMMITTING);

		if (performTermination(transactionHandler)) {
			// certification test has returned true. we can commit.
			commitedTransactions.add(transactionHandler);
			applyWriteSet(transactionHandler);
			applyCreateSet(transactionHandler);
			result.changeState(TransactionState.COMMITTED);

		} else {
			result.changeState(TransactionState.ABORTED_BY_CERTIFICATION);

		}

		garbageCollectTransaction(transactionHandler);
		return result;
	}

	public abstract boolean performTermination(
			TransactionHandler transactionHandler);

	/**
	 * Put the transaction in the aborted list, and does nothing else. TODO
	 * re-execute the transaction.
	 * 
	 * @param transactionHandler
	 */
	public ExecutionHistory abortTransaction(
			TransactionHandler transactionHandler) {
		ExecutionHistory executionHistory = handler2executionHistory
				.get(transactionHandler);
		executionHistory.changeState(TransactionState.ABORTED_BY_CLIENT);
		abortedTransactions.add(transactionHandler);

		return executionHistory;
	}

	/**
	 * Apply changes of a writeSet of a committed transaction to the datastore.
	 * 
	 * @param transactionHandler
	 *            handler of a committed transaction.
	 */
	private void applyWriteSet(TransactionHandler transactionHandler) {
		ExecutionHistory executionHistory = handler2executionHistory
				.get(transactionHandler);

		Iterator<? extends JessyEntity> itr = executionHistory.getWriteSet()
				.getEntities().iterator();

		while (itr.hasNext()) {
			JessyEntity tmp = itr.next();

			// Send the entity to the datastore to be saved
			dataStore.put(tmp);

			// Store the entity as the last committed entity for this particular
			// key.
			lastCommittedEntities.put(tmp.getKey(), tmp);

		}
	}

	/**
	 * Apply changes of a createSet of a committed transaction to the datastore.
	 * 
	 * @param transactionHandler
	 *            handler of a committed transaction.
	 */
	private void applyCreateSet(TransactionHandler transactionHandler) {
		ExecutionHistory executionHistory = handler2executionHistory
				.get(transactionHandler);

		Iterator<? extends JessyEntity> itr = executionHistory.getCreateSet()
				.getEntities().iterator();

		while (itr.hasNext()) {
			JessyEntity tmp = itr.next();

			// Send the entity to the datastore to be saved
			dataStore.put(tmp);

			// Store the entity as the last committed entity for this particular
			// key.
			lastCommittedEntities.put(tmp.getKey(), tmp);

		}
	}

	/**
	 * Executes a non-transactional read on local datastore. This read is
	 * performed on {@link JessyEntity#getSecondaryKey()}
	 * 
	 * @param <E>
	 *            The Type of the entity to read the value from.
	 * @param entityClass
	 *            The Class of the entity to read the value from.
	 * @param keyValue
	 * @return An entity with the secondary key value equals keyValue
	 */
	public <E extends JessyEntity> E read(Class<E> entityClass, String keyValue)
			throws Exception {
		if (transactionalAccess == ExecutionMode.UNDEFINED)
			transactionalAccess = ExecutionMode.NON_TRANSACTIONAL;

		if (transactionalAccess == ExecutionMode.NON_TRANSACTIONAL) {
			return performRead(entityClass, "secondaryKey", keyValue, null);
		}

		throw new Exception(
				"Jessy has been accessed in transactional way. It cannot be accesesed non-transactionally");
	}

	/**
	 * Executes a non-transactional write. Write the entity into the local
	 * datastore This write is performed on
	 * {@link JessyEntity#getSecondaryKey()}
	 * 
	 * @param <E>
	 *            Type of the entity to read the value from.
	 * @param entity
	 *            the object to be written into the local datastore.
	 */
	public <E extends JessyEntity> void write(E entity) throws Exception {
		if (transactionalAccess == ExecutionMode.UNDEFINED)
			transactionalAccess = ExecutionMode.NON_TRANSACTIONAL;

		if (transactionalAccess == ExecutionMode.NON_TRANSACTIONAL) {
			dataStore.put(entity);
			return;
		}

		throw new Exception(
				"Jessy has been accessed in transactional way. It cannot be accesesed non-transactionally");
	}

	private void garbageCollectTransaction(TransactionHandler transactionHandler) {
		handler2executionHistory.remove(transactionHandler);
	}

	public ConcurrentMap<String, JessyEntity> getLastCommittedEntities() {
		return lastCommittedEntities;
	}

	
	
}
