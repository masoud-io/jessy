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

	/**
	 * writeList maps works as follows: ClassName > SecondaryKey > Entity
	 * 
	 */
	ConcurrentMap<String, ConcurrentMap<String, ? extends JessyEntity>> writeSet;

	ConcurrentMap<String, Class<? extends JessyEntity>> classList;

	/**
	 * readKeyList stores all the keys of entities used in read operations as
	 * follows: ClassaName + SecondaryKey
	 */
	CopyOnWriteArrayList<String> readSetKeys;

	/**
	 * stores Vectors of read and write entities.
	 */
	CopyOnWriteArrayList<Vector<String>> readSetVectors;
	CopyOnWriteArrayList<Vector<String>> writeSetVectors;

	public ExecutionHistory() {
		writeSet = new ConcurrentHashMap<String, ConcurrentMap<String, ? extends JessyEntity>>();
		classList = new ConcurrentHashMap<String, Class<? extends JessyEntity>>();

	}

	public <E extends JessyEntity> void addEntity(Class<E> entityClass) {
		// initialize writeList
		writeSet.put(entityClass.toString(),
				new ConcurrentHashMap<String, E>());
		classList.put(entityClass.toString(), entityClass);

		// initialize readLists
		readSetKeys = new CopyOnWriteArrayList<String>();
		readSetVectors = new CopyOnWriteArrayList<Vector<String>>();
		writeSetVectors= new CopyOnWriteArrayList<Vector<String>>();
	}

	public Class<? extends JessyEntity> getEntityClass(String className){
		return classList.get(className);
	}
	
	public List<Vector<String>> getReadSetVectors() {
		return readSetVectors;
	}
	
	public List<Vector<String>> getWriteSetVectors() {
		return writeSetVectors;
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> E getFromWriteSet(Class<E> entityClass,
			String keyValue) {
		Map<String, E> writes = (Map<String, E>) writeSet.get(entityClass
				.toString());
		return writes.get(keyValue);
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

	public <E extends JessyEntity> void addToReadSet(E entity) {
		readSetKeys
				.add(entity.getClass().toString() + entity.getSecondaryKey());
		readSetVectors.add(entity.getLocalVector());
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
}
