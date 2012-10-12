package fr.inria.jessy.store;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VectorFactory;

/**
 * @author Masoud Saeida Ardekani
 * 
 *         This class provides two keys to every object stored by {@link Jessy}
 *         PrimaryKey is of type {@link Long} SecondaryKey is of type
 *         {@link String}
 */

// FIXME transient field ?

@Persistent
public abstract class JessyEntity implements Externalizable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	public static Keyspace keyspace = Keyspace.DEFAULT_KEYSPACE;

	private Vector<String> localVector;
	private boolean removed = true;

	@SuppressWarnings("unused")
	private JessyEntity() {
	}

	public JessyEntity(String entityId) {
		this.secondaryKey = entityId;
		localVector = VectorFactory.getVector(entityId);
	}

	public boolean isRemovoed() {
		return removed;
	}

	/**
	 * Set the entity to be removed by the garbage collector. TODO set
	 * localVector to null to save memory! (safety might be broken)
	 * 
	 */
	public void removoe() {
		this.removed = true;
	}

	@PrimaryKey(sequence = "Jessy_Sequence")
	private PrimaryKeyType primaryKey;

	@SecondaryKey(relate = MANY_TO_ONE)
	private String secondaryKey;

	/**
	 * @param primaryKey
	 *            the primaryKey to set
	 */
	@Deprecated
	public void setPrimaryKey(Long primaryKey) {
		if (primaryKey != null)
			this.primaryKey.primaryKey = primaryKey;
	}

	/**
	 * This key is also the partitioning key.
	 * 
	 * @return
	 */
	public String getKey() {
		return secondaryKey;
	}

	public Vector<String> getLocalVector() {
		return localVector;
	}

	public void setLocalVector(Vector<String> localVector) {
		this.localVector = localVector;
	}

	public void clearValue() {

	}

	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		secondaryKey = (String) in.readObject();
		localVector = (Vector<String>) in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(secondaryKey);
		out.writeObject(localVector);

	}

}
