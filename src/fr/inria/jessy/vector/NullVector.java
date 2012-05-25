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
	public boolean isCompatible(Vector<K> other) throws NullPointerException {
		return true;
	}

	@Override
	public boolean isCompatible(CompactVector<K> other)
			throws NullPointerException {
		return true;
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
