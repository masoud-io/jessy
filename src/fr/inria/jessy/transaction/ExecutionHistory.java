package fr.inria.jessy.transaction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.vector.Vector;

public class ExecutionHistory {

	/**
	 * writeList maps works as follows: ClassName > SecondaryKey > Entity
	 * 
	 */
	ConcurrentMap<String, ConcurrentMap<String, ? extends JessyEntity>> writeList;

	ConcurrentMap<String, Class<? extends JessyEntity>> classList;

	/**
	 * readKeyList stores all the keys of entities used in read operations as
	 * follows: ClassaName + SecondaryKey
	 */
	CopyOnWriteArrayList<String> readKeyList;

	/**
	 * readVectorList stores Vectors of read entities.
	 */
	CopyOnWriteArrayList<Vector<String>> readVectorList;

	public ExecutionHistory() {
		writeList = new ConcurrentHashMap<String, ConcurrentMap<String, ? extends JessyEntity>>();
		classList = new ConcurrentHashMap<String, Class<? extends JessyEntity>>();

	}

	public <E extends JessyEntity> void addEntity(Class<E> entityClass) {
		// initialize readList and writeList
		writeList.put(entityClass.toString(),
				new ConcurrentHashMap<String, E>());
		classList.put(entityClass.toString(), entityClass);

		readKeyList = new CopyOnWriteArrayList<String>();
		readVectorList = new CopyOnWriteArrayList<Vector<String>>();
	}

	public List<Vector<String>> getReadSetVector() {
		return readVectorList;
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> E getFromWriteSet(Class<E> entityClass,
			String keyValue) {
		Map<String, E> writes = (Map<String, E>) writeList.get(entityClass
				.toString());
		return writes.get(keyValue);
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> boolean writeSetContains(
			Class<E> entityClass, String keyValue) {
		Map<String, E> writes = (Map<String, E>) writeList.get(entityClass
				.toString());
		E entity = writes.get(keyValue);
		if (entity != null) {
			return true;
		} else {
			return false;
		}
	}

	public <E extends JessyEntity> void addToReadSet(E entity) {
		readKeyList
				.add(entity.getClass().toString() + entity.getSecondaryKey());
		readVectorList.add(entity.getLocalVector());
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> void addToWriteSet(E entity) {
		ConcurrentMap<String, E> writes = (ConcurrentMap<String, E>) writeList
				.get(entity.getClass().toString());
		writes.put(entity.getSecondaryKey(), entity);
		writeList.put(entity.getClass().toString(), writes);
	}
}
