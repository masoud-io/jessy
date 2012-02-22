package fr.inria.jessy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.vector.Vector;

public class ExecutionHistory {

	/**
	 * readList and writeList maps works as follows: ClassName > SecondaryKey >
	 * Entity
	 */
	Map<String, Map<String, ? extends JessyEntity>> readList;
	Map<String, Map<String, ? extends JessyEntity>> writeList;
	Map<String, Class<? extends JessyEntity>> classList;
	List<Vector<String>> readVectors;

	public ExecutionHistory() {
		writeList = new HashMap<String, Map<String, ? extends JessyEntity>>();
		readList = new HashMap<String, Map<String, ? extends JessyEntity>>();
		classList = new HashMap<String, Class<? extends JessyEntity>>();
		readVectors = new ArrayList<Vector<String>>();
	}

	public <E extends JessyEntity> void addEntity(Class<E> entityClass) {
		// initialize readList and writeList
		readList.put(entityClass.toString(), new HashMap<String, E>());
		writeList.put(entityClass.toString(), new HashMap<String, E>());
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
		Map<String, E> writes = (Map<String, E>) writeList.get(entity
				.getClass().toString());
		writes.put(entity.getSecondaryKey(), entity);
		writeList.put(entity.getClass().toString(), writes);
	}
}
