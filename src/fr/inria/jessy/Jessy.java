package fr.inria.jessy;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import fr.inria.jessy.consistency.Consistency;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.Vector;

/**
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
	private ExecutionHistory executionHistoryTemplate;

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

		executionHistoryTemplate = new ExecutionHistory();

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
	 * All entities that will be managed by Jessy middle ware should be
	 * introduced to it beforehand. Currently, it only supports to add one
	 * primary key and one secondary key for each entity.
	 * 
	 * @param <E>
	 *            Type of the entity that will be store/retrieve
	 * @param <SK>
	 *            Type of the secondary key. This can usually be String
	 * @param entityClass
	 *            Class of the entity that will be store/retrieve
	 * @param secondaryKeyClass
	 *            Class of the secondary key. This can usually be String
	 * @param secondaryKeyName
	 *            Name of the field that read request will be issued on.
	 * @throws Exception
	 */
	// TODO write a function for adding extra secondary keys!
	private <E extends JessyEntity, SK> void addEntity(Class<E> entityClass,
			Class<SK> secondaryKeyClass, String secondaryKeyName)
			throws Exception {
		dataStore.addPrimaryIndex(entityClass);
		dataStore.addSecondaryIndex(entityClass, secondaryKeyClass,
				secondaryKeyName);

		executionHistoryTemplate.addEntity(entityClass);

	}

	public <E extends JessyEntity, SK> void addEntity(Class<E> entityClass)
			throws Exception {
		addEntity(entityClass, String.class, "secondaryKey");
	}

	/**
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
	 * @return An entity with the secondary key value equals keyValue
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
		entity = executionHistory.getFromWriteSet(entityClass, keyValue);

		// we first check it this entity has been updated in this transaction
		// before!
		if (entity == null) {

			// if the entity has not been updated, we check if it has been read
			// in the same transaction before.
			entity = executionHistory.getFromReadSet(entityClass, keyValue);

			if (entity == null)
				entity = performRead(entityClass, "secondaryKey", keyValue,
						executionHistory.getReadSetVectors());
		}

		if (entity != null) {
			executionHistory.addToReadSet(entity);
			return entity;
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
			List<Vector<String>> readList);

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
		} else
			executionHistory.addToWriteSet(entity);
	}

	/**
	 * Creates an entity in the system. 
	 * It simply calls the write method, and passes the arguments.
	 * TODO Read_Before_Write logic cannot be ensured for this method since there is no entity to be read before.
	 * TODO one solution is to add version zero of all entities! 
	 */
	public <E extends JessyEntity> void create(
			TransactionHandler transactionHandler, E entity){
		write(transactionHandler, entity);
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
			handler2executionHistory.put(transactionHandler,
					executionHistoryTemplate);
			return transactionHandler;

		}
		throw new Exception(
				"Jessy has been accessed in non-transactional way. It cannot be accesesed transactionally");
	}

	/**
	 * Commit the open transaction.
	 * 
	 * @return
	 */
	public boolean commitTransaction(TransactionHandler transactionHandler) {

		if (terminateTransaction(transactionHandler)) {
			// certification test has returned true. we can commit.
			commitedTransactions.add(transactionHandler);
			applyUpdates(transactionHandler);
			return true;
		}
		return false;

	}

	public abstract boolean terminateTransaction(
			TransactionHandler transactionHandler);

	public void abortTransaction(TransactionHandler transactionHandler) {
		abortedTransactions.add(transactionHandler);
	}

	/**
	 * Apply changes of a committed transaction to the datastore.
	 * 
	 * @param transactionHandler
	 *            handler of a committed transaction.
	 */
	public void applyUpdates(TransactionHandler transactionHandler) {
		ExecutionHistory executionHistory = handler2executionHistory
				.get(transactionHandler);

		Iterator<? extends JessyEntity> itr = executionHistory.getWriteSet()
				.iterator();

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
	 *            Type of the entity to read the value from.
	 * @param entityClass
	 *            Class of the entity to read the value from.
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
	 * Write the entity into the local datastore This write is performed on
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
		}

		throw new Exception(
				"Jessy has been accessed in transactional way. It cannot be accesesed non-transactionally");
	}

}
