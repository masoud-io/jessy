package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;

/**
 * @author Masoud Saeida Ardekani This class implements Vector used in
 *         [Peluso2012].
 * 
 */

@Persistent
public class GMUVector<K> extends Vector<K> implements Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;

	/**
	 * Needed for BerkeleyDB
	 */
	@Deprecated
	public GMUVector() {
		super();
	}

	public GMUVector(K selfKey) {
		super(selfKey);
		super.setValue(selfKey, 0);
	}

	@Override
	public CompatibleResult isCompatible(Vector<K> other)
			throws NullPointerException {
		// check special values
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		Integer selfValueOnSelfKey = getSelfValue();
		Integer otherValueOnSelfKey = other.getValue(selfKey);

		Integer selfValueOnOtherKey = getValue(other.getSelfKey());
		Integer otherValueOnOtherKey = other.getSelfValue();

		if (selfValueOnSelfKey >= otherValueOnSelfKey
				&& otherValueOnOtherKey >= selfValueOnOtherKey) {
			return Vector.CompatibleResult.COMPATIBLE;
		}

		return Vector.CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
	}

	/**
	 * This implementatino corresponds to Algorithm 2 in the paper
	 */
	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {
		HashMap<K, Boolean> hasRead = (HashMap<K, Boolean>) other
				.getExtraObject();

		Integer maxVCAti;

		if (!hasRead.get(getSelfKey())) {
			/*
			 * line 3,4
			 */
			for (K index : hasRead.keySet()) {
				if (getValue(index) > other.getValue(index))
					return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
			}
			maxVCAti = getValue(getSelfKey());
		} else {
			maxVCAti = other.getValue(getSelfKey());
		}

		if (getSelfValue() <= maxVCAti)
			return CompatibleResult.COMPATIBLE;
		else
			return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;

	}

	/**
	 * update method gets two {@link CompactVector} as readSet and writeSet and
	 * applies them into its local vector. <b> this implementation does not
	 * assume read before write rule <\b>
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

	@Override
	public GMUVector<K> clone() {
		return (GMUVector<K>) super.clone();
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}

	@Override
	public boolean requireExtraObjectInCompactVector() {
		return true;
	}

	@Override
	public void updateExtraObjectInCompactVector(Object object) {
		((HashMap<K, Boolean>) object).put(selfKey, true);
	}
}
