package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.util.Map;

import com.sleepycat.persist.model.Persistent;

/**
 * A classical version vector. To be used by PSI.
 * 
 * Each object version holds a Vector. Moreover, there exists a static
 * ValueVector called {@code observedCommittedTransactions}. SelfKey will be
 * used in this class as the index of the server. In other words,
 * Valueof(SelfKey) returns the number of committed transactions in this server.
 * 
 * @author Masoud Saeida Ardekani
 * 
 * @param <K>
 */
@Persistent
public class VersionVector<K> extends Vector<K> implements Externalizable {

	/**
	 * this Vector plays the role of a vector assigned to each jessy server in
	 * the system.
	 */
	public static ValueVector<String, Integer> observedCommittedTransactions = new ValueVector<String, Integer>();

	public VersionVector(K selfKey) {
		super(selfKey);
		super.setValue(selfKey, 0);
	}

	/**
	 * Checks if the input vector is compatible with this vector.
	 * <p>
	 * WARNING: it is correct only if is called from the very last version up to
	 * the first.
	 */
	@Override
	public boolean isCompatible(Vector<K> other) throws NullPointerException {
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		if (this.getValue(selfKey).equals(other.getValue(selfKey)))
			return true;
		else
			return false;

	}

	/**
	 * TODO This implementation is not safe. It is only safe when all updated
	 * messages have been applied to local datastore!
	 */
	@Override
	public boolean isCompatible(CompactVector<K> other)
			throws NullPointerException {

		if (other == null)
			throw new NullPointerException("Input Vector is Null");

		if (other.size() == 0)
			return true;

		if (getValue(selfKey) < other.getValue(selfKey))
			return true;
		else
			return false;
	}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {
		// Readset can be simply applied by using the update method of
		// ValueVector Class
		if (readSet.size() > 0)
			super.update(readSet);

		// Write set is more involved.
		Integer value;

		for (Map.Entry<K, Integer> entry : writeSet.getEntrySet()) {
			K key = entry.getKey();
			value = entry.getValue();
			if (writeSet.getKeys().contains(key))
				value++;
			if (getValue(key).compareTo(value) < 0) {
				setValue(key, value);
			}
		}
	}

}
