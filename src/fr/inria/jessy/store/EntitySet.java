package fr.inria.jessy.store;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sourceforge.fractal.Messageable;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.vector.CompactVector;

/**
 * @author Masoud Saeida Ardekani
 * 	
 *  This class maintains a list of entities read
 *  or written by a transaction. It is fundamental to
 *  {@link ExecutionHistory}
 * 
 */
public class EntitySet implements Messageable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	/**
	 * maps works as follows: ClassName > SecondaryKey > Entity
	 * 
	 */
	private Map<String, Map<String, ? extends JessyEntity>> entities;

	private CompactVector<String> compactVector;

	public EntitySet() {
		entities = new HashMap<String, Map<String, ? extends JessyEntity>>();
		compactVector = new CompactVector<String>();
	}

	public synchronized <E extends JessyEntity> void addEntityClass(Class<E> entityClass) {
		// initialize writeList
		entities.put(entityClass.toString(), new HashMap<String, E>());
	}

	public CompactVector<String> getCompactVector() {
		return compactVector;
	}

	@SuppressWarnings("unchecked")
	public synchronized <E extends JessyEntity> E getEntity(Class<E> entityClass,
			String keyValue) {
		Map<String, E> writes = (Map<String, E>) entities.get(entityClass
				.toString());
		return writes.get(keyValue);
	}

	@SuppressWarnings("unchecked")
	public synchronized <E extends JessyEntity> void addEntity(E entity) {
		compactVector.update(entity.getLocalVector());
		if(!entities.containsKey(entity.getClass().toString()))
			entities.put(entity.getClass().toString(), new HashMap<String, JessyEntity>());
		Map<String, E> temp = (Map<String, E>) entities.get(entity.getClass().toString());
		temp.put(entity.getKey(), entity);
		entities.put(entity.getClass().toString(), temp);
	}

	@SuppressWarnings("unchecked")
	public synchronized <E extends JessyEntity> void addEntity(Collection<E> entityCol) {
		for (E entity : entityCol) {
			addEntity(entity);
		}
	}

	public synchronized void addEntity(EntitySet entitySet) {
		Iterator<? extends JessyEntity> itr = entitySet.getEntities()
				.iterator();
		while (itr.hasNext()) {
			JessyEntity jessyEntity = itr.next();
			addEntity(jessyEntity);
		}

	}

	public synchronized List<? extends JessyEntity> getEntities() {
		List<JessyEntity> result = new ArrayList<JessyEntity>();

		Collection<Map<String, ? extends JessyEntity>> writeListValues = entities
				.values();

		Iterator<Map<String, ? extends JessyEntity>> itr = writeListValues
				.iterator();
		while (itr.hasNext()) {
			Map<String, ? extends JessyEntity> entities = itr.next();
			result.addAll(entities.values());
		}

		return result;
	}

	public int size() {
		return compactVector.size();
	}

	@SuppressWarnings("unchecked")
	public synchronized <E extends JessyEntity> boolean contains(Class<E> entityClass,
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

	public String toString() {
		String result = "";

		Iterator<? extends JessyEntity> itr = getEntities().iterator();
		while (itr.hasNext()) {
			JessyEntity temp = itr.next();
			result = result + "--" + temp.getLocalVector() ;
		}

		return result;
	}

	// FIXME Performance Bottleneck. There are so many loops here.
	public Set<String> getKeys() {
		Set<String> keys = new HashSet<String>();
		List<? extends JessyEntity>  entityList = getEntities();
		for (JessyEntity e : entityList) {
			keys.add(e.getKey());
		}
		return keys;
	}

	public void clear() {
		entities.clear();
		compactVector = new CompactVector<String>();
	}

	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		entities = (Map<String, Map<String, ? extends JessyEntity>>) in.readObject();
		compactVector = (CompactVector<String>) in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(entities);
		out.writeObject(compactVector);
	}
}
