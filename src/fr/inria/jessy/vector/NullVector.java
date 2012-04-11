package fr.inria.jessy.vector;

import java.io.Serializable;
import java.util.List;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;

@Persistent
public class NullVector<K> extends Vector<K> implements Serializable{

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
	public boolean isCompatible(List<Vector<K>> otherList) throws NullPointerException {
		return true;
	}

	@Override
	public void update(List<Vector<K>> readList, List<Vector<K>> writeList) {}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {}

}
