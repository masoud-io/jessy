package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;

/**
 * @author Masoud Saeida Ardekani This class implements dependence vector for
 *         jessy objects.
 * 
 *         TODO WRITE DOCs
 */

@Persistent
public class DependenceVector<K> extends Vector<K> implements Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;

	/**
	 * Needed for BerkeleyDB 
	 */
	@Deprecated
	public DependenceVector() {
		super();
	}

	public DependenceVector(K selfKey) {
		super(selfKey);
		super.setValue(selfKey, 0);
	}

	@Override
	public CompatibleResult isCompatible(Vector<K> other) throws NullPointerException {
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

		return Vector.CompatibleResult.NOT_COMPATIBLE;
	}

	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {
		// check special values

		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		if (other.size() == 0)
			return Vector.CompatibleResult.COMPATIBLE;

		Integer selfValueOnSelfKey = getSelfValue();
		Integer otherValueOnSelfKey = other.getValue(selfKey);

		if (selfValueOnSelfKey < otherValueOnSelfKey) {
			return Vector.CompatibleResult.NOT_COMPATIBLE;
		}

		Integer selfValueOnOtherKey;
		Integer otherValueOnOtherKey;

		for (K k : other.getKeys()) {
			selfValueOnOtherKey = getValue(k);
			otherValueOnOtherKey = other.getValue(k);

			if (otherValueOnOtherKey < selfValueOnOtherKey) {
				return Vector.CompatibleResult.NOT_COMPATIBLE;
			}

		}

		return Vector.CompatibleResult.COMPATIBLE;
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
	public DependenceVector<K> clone() {
		return (DependenceVector<K>) super.clone();
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}
}


//@Override
//public boolean isCompatible(List<Vector<K>> otherList)
//		throws NullPointerException {
//	// check special values
//	if (otherList == null) {
//		throw new NullPointerException("Input Vector is Null");
//	}
//
//	for (Vector<K> other : otherList) {
//		if (isCompatible(other) == false)
//			return false;
//	}
//	return true;
//}
//
//@Override
//public void update(List<Vector<K>> readList, List<Vector<K>> writeList) {
//	for (Vector<K> readVector : readList) {
//		super.update(readVector);
//	}
//
//	for (Vector<K> writeVector : writeList) {
//		super.update(writeVector);
//		super.setValue(writeVector.getSelfKey(),
//				writeVector.getSelfValue() + 1);
//	}
//}