package fr.inria.jessy.transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.vector.Vector;

public class ExecutionHistory {

	public enum TransactionType {
		/**
		 * the execution history is for a read only transaction
		 */
		READONLY_TRANSACTION,
		/**
		 * the execution history is for an update transaction
		 */
		UPDATE_TRANSACTION
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
		 * the transaction has been aborted
		 */
		ABORTED
	};

	private TransactionState transactionState;
	
	/**
	 * readSet and writeSet maps works as follows: ClassName > SecondaryKey >
	 * Entity
	 * 
	 */
	private ConcurrentMap<String, ConcurrentMap<String, ? extends JessyEntity>> writeSet;
	private ConcurrentMap<String, ConcurrentMap<String, ? extends JessyEntity>> readSet;

	/**
	 * stores Vectors of read and write entities. This is for fast retrieval.
	 * TODO if the performance is good enough, writeSetVectors can be removed.
	 * It can directly be computed from {@link ExecutionHistory#getWriteSet()}
	 */
	private CopyOnWriteArrayList<Vector<String>> readSetVectors;
	private CopyOnWriteArrayList<Vector<String>> writeSetVectors;

	public ExecutionHistory() {
		readSet = new ConcurrentHashMap<String, ConcurrentMap<String, ? extends JessyEntity>>();
		readSetVectors = new CopyOnWriteArrayList<Vector<String>>();

		writeSet = new ConcurrentHashMap<String, ConcurrentMap<String, ? extends JessyEntity>>();
		writeSetVectors = new CopyOnWriteArrayList<Vector<String>>();
	}

	public <E extends JessyEntity> void addEntity(Class<E> entityClass) {
		// initialize writeList
		writeSet.put(entityClass.toString(), new ConcurrentHashMap<String, E>());
		readSet.put(entityClass.toString(), new ConcurrentHashMap<String, E>());

	}

	public List<Vector<String>> getReadSetVectors() {
		return readSetVectors;
	}

	public List<Vector<String>> getWriteSetVectors() {
		return writeSetVectors;
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> E getFromReadSet(Class<E> entityClass,
			String keyValue) {
		Map<String, E> reads = (Map<String, E>) readSet.get(entityClass
				.toString());
		return reads.get(keyValue);
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> E getFromWriteSet(Class<E> entityClass,
			String keyValue) {
		Map<String, E> writes = (Map<String, E>) writeSet.get(entityClass
				.toString());
		return writes.get(keyValue);
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> boolean readSetContains(
			Class<E> entityClass, String keyValue) {

		Map<String, E> reads = (Map<String, E>) readSet.get(entityClass
				.toString());
		E entity = reads.get(keyValue);
		if (entity != null) {
			return true;
		} else {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> boolean writeSetContains(
			Class<E> entityClass, String keyValue) {

		Map<String, E> writes = (Map<String, E>) writeSet.get(entityClass
				.toString());
		E entity = writes.get(keyValue);
		if (entity != null) {
			return true;
		} else {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> void addToReadSet(E entity) {
		readSetVectors.add(entity.getLocalVector());

		ConcurrentMap<String, E> reads = (ConcurrentMap<String, E>) readSet
				.get(entity.getClass().toString());
		reads.put(entity.getSecondaryKey(), entity);
		readSet.put(entity.getClass().toString(), reads);
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> void addToWriteSet(E entity) {
		writeSetVectors.add(entity.getLocalVector());

		ConcurrentMap<String, E> writes = (ConcurrentMap<String, E>) writeSet
				.get(entity.getClass().toString());
		writes.put(entity.getSecondaryKey(), entity);
		writeSet.put(entity.getClass().toString(), writes);
	}

	public List<? extends JessyEntity> getWriteSet() {
		List<JessyEntity> result = new ArrayList<JessyEntity>();

		Collection<ConcurrentMap<String, ? extends JessyEntity>> writeListValues = writeSet
				.values();

		Iterator<ConcurrentMap<String, ? extends JessyEntity>> itr = writeListValues
				.iterator();
		while (itr.hasNext()) {
			ConcurrentMap<String, ? extends JessyEntity> entities = itr.next();
			result.addAll(entities.values());
		}

		return result;
	}

	public TransactionType getTransactionType() {
		if (writeSet.size() > 0)
			return TransactionType.UPDATE_TRANSACTION;
		else
			return TransactionType.READONLY_TRANSACTION;
	}

	public TransactionState getTransactionState() {
		return transactionState;
	}

	public void setTransactionState(TransactionState transactionState) {
		this.transactionState = transactionState;
	}
	
	
}
