package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import com.sleepycat.persist.model.Persistent;

;

@Persistent
public abstract class Vector<K> extends ValueVector<K, Integer> implements Externalizable{

	K selfKey;
	private final static Integer _bydefault=-1;

	public Vector() {
		super(_bydefault);
	}

	public Vector(K selfKey) {
		super(_bydefault);
		this.selfKey = selfKey;
	}

	public abstract boolean isCompatible(Vector<K> other)
			throws NullPointerException;

	public abstract boolean isCompatible(CompactVector<K> other)
			throws NullPointerException;

	public abstract boolean isCompatible(List<Vector<K>> otherList)
			throws NullPointerException;

	public abstract void update(List<Vector<K>> readList,
			List<Vector<K>> writeList);

	public abstract void update(CompactVector<K> readSet,
			CompactVector<K> writeSet);

	public K getSelfKey() {
		return selfKey;
	}

	public void setSelfKey(K selfKey) {
		this.selfKey = selfKey;
	}

	public Integer getSelfValue() {
		return super.getValue(selfKey);
	}

	public Vector<K> clone() {
		Vector<K> result = (Vector<K>) super.clone();
		result.selfKey = selfKey;

		return result;
	}

	/**
	 * Increament the value of selfKey;
	 */
	public void increament() {
		setValue(selfKey, (getSelfValue() + 1));
	}

	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		super.setBydefault(_bydefault);
		selfKey = (K) in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(selfKey);
	}
}
