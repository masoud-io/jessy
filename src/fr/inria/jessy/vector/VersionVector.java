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
 * used in this class as the index of the group of jessy instances that plays
 * the role of the transaction coordinator. E.g., Consider jessy instances p1
 * and p2 replicate entity x, and belongs to group g1. If transaction T1
 * modifies entity x, the the selfkey of committed version of entity x is g1.
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
	public static ConcurrentVersionVector<String> committedVTS = new ConcurrentVersionVector<String>(
			JessyGroupManager.getInstance().getMyGroup().name());

	@Deprecated
	public VersionVector() {
	}

	public VersionVector(K selfKey, Integer selfValue) {
		super(selfKey);
		super.setValue(selfKey, selfValue);
	}

	/**
	 * @inheritDoc
	 * 
	 *             WARNING: it is correct only if is called from the very last
	 *             version up to the first.
	 */
	@Override
	public CompatibleResult isCompatible(Vector<K> other)
			throws NullPointerException {
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		if (this.getValue(selfKey).compareTo(other.getValue(selfKey)) <= 0)
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
			/*
			 * if other vector size is zero, then this is the very first read.
			 * Thus, we set the StartVTS
			 */
			this.setMap(new HashMap<K, Integer>((Map) committedVTS.getVector()));
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

	}

}
