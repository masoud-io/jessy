package fr.inria.jessy.store;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.sourceforge.fractal.Messageable;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.utils.Compress;
import fr.inria.jessy.vector.CompactVector;

/**
 * @author Masoud Saeida Ardekani
 * 
 *         This class maintains a list of entities read or written by a
 *         transaction. It is fundamental to {@link ExecutionHistory}
 *         <p>
 *         Note: Throughout this class, ClassName is compressed using
 *         {@code Compress#compressClassName(String)} method.
 *         <p>
 *         This class is not ThreadSafe. It should only be accessed by one
 *         thread.
 * 
 */
public class EntitySet implements Messageable{

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	/**
	 * maps works as follows: (Compressed ClassName + SecondaryKey) > Entity
	 */
	private Map<String, Object> entities;

	private CompactVector<String> compactVector;

	public EntitySet() {
		entities = new HashMap<String, Object>();
		compactVector = new CompactVector<String>();
	}

	public CompactVector<String> getCompactVector() {
		return compactVector;
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> E getEntity(Class<E> entityClass,
			String keyValue) {
		return (E) entities.get(Compress.compressClassName(entityClass
				.getName()) + keyValue);
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> void addEntity(E entity) {
		compactVector.update(entity.getLocalVector());
		entities.put(Compress.compressClassName(entity.getClass().getName())
				+ entity.getKey(), entity);
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> void addEntity(Collection<E> entityCol) {
		for (E entity : entityCol) {
			addEntity(entity);
		}
	}

	public void addEntity(EntitySet entitySet) {
		Iterator<? extends JessyEntity> itr = entitySet.getEntities()
				.iterator();
		while (itr.hasNext()) {
			JessyEntity jessyEntity = itr.next();
			addEntity(jessyEntity);
		}

	}

	public Collection<? extends JessyEntity> getEntities() {
		return (Collection<? extends JessyEntity>) entities.values();
	}

	public int size() {
		return compactVector.size();
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> boolean contains(
			Class<E> entityClass, String keyValue) {

		return entities.containsKey(Compress.compressClassName(entityClass
				.getName() + keyValue));

	}

	public String toString() {
		String result = "";

		Iterator<? extends JessyEntity> itr = getEntities().iterator();
		while (itr.hasNext()) {
			JessyEntity temp = itr.next();
			result = result + "--" + temp.getLocalVector();
		}

		return result;
	}

	public Set<String> getKeys() {
		return entities.keySet();
	}

	public void clear() {
		entities.clear();
		compactVector = new CompactVector<String>();
	}

	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		entities = (Map<String, Object>) in.readObject();
		compactVector = (CompactVector<String>) in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(entities);
		out.writeObject(compactVector);
	}
}
