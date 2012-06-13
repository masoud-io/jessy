package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.communication.JessyGroupManager;

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
	public static VersionVector<String> observedCommittedTransactions = new VersionVector<String>(
			JessyGroupManager.getInstance().getMyGroup().name());

	@Deprecated
	public VersionVector() {
	}

	public VersionVector(K selfKey) {
		super(selfKey);
		super.setValue(selfKey, 0);
	}

	/**
	 * @inheritDoc
	 * 
	 *             WARNING: it is correct only if is called from the very last
	 *             version up to the first.
	 */
	@Override
	public CompatibleResult isCompatible(Vector<K> other) throws NullPointerException {
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		if (this.getValue(selfKey).equals(other.getValue(selfKey)))
			return Vector.CompatibleResult.COMPATIBLE;
		else
			return Vector.CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;

	}

	/**
	 * @inheritDoc
	 * 
	 *             TODO This implementation is not safe. It is only safe when
	 *             all updated messages have been applied to local datastore!
	 */
	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {

		if (other == null)
			throw new NullPointerException("Input Vector is Null");

		if (other.size() == 0) {
			this.setMap((HashMap) observedCommittedTransactions.getMap());
			return Vector.CompatibleResult.COMPATIBLE;
		}

		if (getValue(selfKey) <= other.getValue(selfKey))
			return Vector.CompatibleResult.COMPATIBLE;
		else
			return Vector.CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
	}

	/**
	 * @inheritDoc
	 */
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
