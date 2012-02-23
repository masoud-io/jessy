package fr.inria.transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.vector.Vector;

public class ExecutionHistory {

	/**
	 * readList and writeList maps works as follows: ClassName > SecondaryKey >
	 * Entity
	 */
	ConcurrentMap<String, ConcurrentMap<String, ? extends JessyEntity>> readList;
	ConcurrentMap<String, ConcurrentMap<String, ? extends JessyEntity>> writeList;
	ConcurrentMap<String, Class<? extends JessyEntity>> classList;
	List<Vector<String>> readVectors;

	public ExecutionHistory() {
		writeList = new ConcurrentHashMap<String, ConcurrentMap<String, ? extends JessyEntity>>();
		readList = new ConcurrentHashMap<String, ConcurrentMap<String, ? extends JessyEntity>>();
		classList = new ConcurrentHashMap<String, Class<? extends JessyEntity>>();
		readVectors = new ArrayList<Vector<String>>();
	}

	public <E extends JessyEntity> void addEntity(Class<E> entityClass) {
		// initialize readList and writeList
		readList.put(entityClass.toString(), new ConcurrentHashMap<String, E>());
		writeList.put(entityClass.toString(), new ConcurrentHashMap<String, E>());
		classList.put(entityClass.toString(), entityClass);
	}


	public List<Vector<String>> getReadSetVector(){
		return readVectors;
	}
	
	public <E extends JessyEntity> E getFromWriteSet(
			Class<E> entityClass, String keyValue) {
		Map<String, E> writes = (Map<String, E>) writeList.get(entityClass
				.toString());
		return writes.get(keyValue);
	}

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
		readVectors.add(entity.getLocalVector());
	}	
	
	public <E extends JessyEntity> void addToWriteSet(E entity) {
		ConcurrentMap<String, E> writes = (ConcurrentMap<String, E>) writeList.get(entity
				.getClass().toString());
		writes.put(entity.getSecondaryKey(), entity);
		writeList.put(entity.getClass().toString(), writes);
	}
}
