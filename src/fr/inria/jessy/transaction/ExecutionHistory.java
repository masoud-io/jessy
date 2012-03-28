package fr.inria.jessy.transaction;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import fr.inria.jessy.store.EntitySet;
import fr.inria.jessy.store.JessyEntity;

public class ExecutionHistory {

	public enum TransactionType {
		/**
		 * the execution history is for a read only transaction
		 */
		READONLY_TRANSACTION,
		/**
		 * the execution history is for an update transaction
		 */
		UPDATE_TRANSACTION,
		/**
		 * the execution history is for an initialization transaction
		 */
		INIT_TRANSACTION
	};

	public enum TransactionState {
		/**
		 * the transaction has not strated yet.
		 */
		NOT_STARTED,
		/**
		 * the transaction has been started and is executing
		 */
		EXECUTING,
		/**
		 * the transaction has been executed and is committing
		 */
		COMMITTING,
		/**
		 * the transaction has been committed
		 */
		COMMITTED,
		/**
		 * the transaction has been aborted because of the certification test
		 */
		ABORTED_BY_CERTIFICATION,
		/**
		 * the transaction has been aborted by the client.
		 */
		ABORTED_BY_CLIENT,
	};

	private TransactionState transactionState = TransactionState.NOT_STARTED;

	private ConcurrentMap<TransactionState, Long> transactionState2StartingTime;

	/**
	 * readSet and writeSet to store read and written entities
	 * 
	 */

	private EntitySet createSet;
	private EntitySet writeSet;
	private EntitySet readSet;

	public ExecutionHistory(List<Class<? extends JessyEntity>> entityClasses) {
		readSet = new EntitySet();

		writeSet = new EntitySet();

		createSet = new EntitySet();

		transactionState2StartingTime = new ConcurrentHashMap<ExecutionHistory.TransactionState, Long>();

		for (Class<? extends JessyEntity> entityClass : entityClasses) {
			addEntityClass(entityClass);
		}

	}

	private <E extends JessyEntity> void addEntityClass(Class<E> entityClass) {
		// initialize writeList
		readSet.addEntityClass(entityClass);
		writeSet.addEntityClass(entityClass);
		createSet.addEntityClass(entityClass);
	}

	public EntitySet getReadSet() {
		return readSet;
	}

	public EntitySet getWriteSet() {
		return writeSet;
	}

	public EntitySet getCreateSet() {
		return createSet;
	}

	public <E extends JessyEntity> E getReadEntity(Class<E> entityClass,
			String keyValue) {
		return readSet.getEntity(entityClass, keyValue);
	}

	public <E extends JessyEntity> E getWriteEntity(Class<E> entityClass,
			String keyValue) {
		return writeSet.getEntity(entityClass, keyValue);
	}

	public <E extends JessyEntity> E getCreateEntity(Class<E> entityClass,
			String keyValue) {
		return createSet.getEntity(entityClass, keyValue);
	}

	public <E extends JessyEntity> void addReadEntity(E entity) {
		readSet.addEntity(entity);
	}

	public <E extends JessyEntity> void addReadEntity(Collection<E> entities) {
		readSet.addEntity(entities);
	}

	public <E extends JessyEntity> void addWriteEntity(E entity) {
		writeSet.addEntity(entity);
	}

	public <E extends JessyEntity> void addCreateEntity(E entity) {
		createSet.addEntity(entity);
	}

	public TransactionType getTransactionType() {
		if (createSet.size() > 0 && writeSet.size() == 0 && readSet.size() == 0)
			return TransactionType.INIT_TRANSACTION;
		else if (writeSet.size() > 0)
			return TransactionType.UPDATE_TRANSACTION;
		else
			return TransactionType.READONLY_TRANSACTION;
	}

	public TransactionState getTransactionState() {
		return transactionState;
	}

	public void changeState(TransactionState transactionNewState) {
		transactionState = transactionNewState;
		transactionState2StartingTime.put(transactionState,
				System.currentTimeMillis());
	}

	public String toString() {
		String result;
		result = transactionState.toString() + "\n";
		result = result + readSet.toString() + "\n";
		result = result + writeSet.toString() + "\n";
		return result;
	}
}
