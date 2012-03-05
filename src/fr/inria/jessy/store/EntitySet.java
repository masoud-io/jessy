/**
 * 
 */
package fr.inria.jessy.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.Vector;

/**
 * @author Masoud Saeida Ardekani This class maintains a list of entities read
 *         or written by a transaction. It is fundamental to
 *         {@link ExecutionHistory}
 * 
 */
public class EntitySet {

	/**
	 * maps works as follows: ClassName > SecondaryKey > Entity
	 * 
	 */
	private ConcurrentMap<String, ConcurrentMap<String, ? extends JessyEntity>> entities;

	private CompactVector<String> compactVector;

	public EntitySet() {
		entities = new ConcurrentHashMap<String, ConcurrentMap<String, ? extends JessyEntity>>();
		compactVector = new CompactVector<String>();
	}

	public <E extends JessyEntity> void addEntityClass(Class<E> entityClass) {
		// initialize writeList
		entities.put(entityClass.toString(), new ConcurrentHashMap<String, E>());
	}

	public CompactVector<String> getCompactVector() {
		return compactVector;
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> E getEntity(Class<E> entityClass,
			String keyValue) {
		Map<String, E> writes = (Map<String, E>) entities.get(entityClass
				.toString());
		return writes.get(keyValue);
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> void addEntity(E entity) {
		compactVector.update(entity.getLocalVector());

		ConcurrentMap<String, E> temp = (ConcurrentMap<String, E>) entities
				.get(entity.getClass().toString());
		temp.put(entity.getSecondaryKey(), entity);

		entities.put(entity.getClass().toString(), temp);
	}

	public List<? extends JessyEntity> getEntities() {
		List<JessyEntity> result = new ArrayList<JessyEntity>();

		Collection<ConcurrentMap<String, ? extends JessyEntity>> writeListValues = entities
				.values();

		Iterator<ConcurrentMap<String, ? extends JessyEntity>> itr = writeListValues
				.iterator();
		while (itr.hasNext()) {
			ConcurrentMap<String, ? extends JessyEntity> entities = itr.next();
			result.addAll(entities.values());
		}

		return result;
	}

	public int size() {
		return compactVector.size();
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> boolean contains(Class<E> entityClass,
			String keyValue) {

		Map<String, E> temp = (Map<String, E>) entities.get(entityClass
				.toString());
		E entity = temp.get(keyValue);
		if (entity != null) {
			return true;
		} else {
			return false;
		}
	}
	
	public String toString(){
		String result="";
		
		Iterator<? extends JessyEntity> itr=getEntities().iterator();
		while (itr.hasNext()){
			JessyEntity temp=itr.next();
			result=temp.getKey() + "--" + temp.getLocalVector() +  "\n";
		}
		
		return result;
	}

}
