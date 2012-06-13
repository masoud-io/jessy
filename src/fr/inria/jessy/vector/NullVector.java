package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;

@Persistent
public class NullVector<K> extends Vector<K> implements Externalizable{

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;
	
	public NullVector() {
		super();
	}

	public NullVector(K selfKey) {
		super();
	}

	
	@Override
	public CompatibleResult isCompatible(Vector<K> other) throws NullPointerException {
		return Vector.CompatibleResult.COMPATIBLE;
	}

	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {
		return Vector.CompatibleResult.COMPATIBLE;
	}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {}
	
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}

}
