package fr.inria.jessy.vector;

import java.util.List;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.vector.ValueVector;;

@Persistent
public abstract class Vector<K> extends ValueVector<K,Integer>{

	public Vector(){
		super(-1);		
	}
	
	public abstract  boolean isReadable(Vector<K> other) throws NullPointerException;

	public abstract boolean isReadable(List<Vector<K>> otherList)
			throws NullPointerException;

	public abstract void update(List<Vector<K>> readList, List<Vector<K>> writeList);

	
	K selfKey;

	public Vector(K selfKey) {
		super(-1);
		this.selfKey = selfKey;
	}
	
	public K getSelfKey() {
		return selfKey;
	}

	public Integer getSelfValue() {
		return super.getValue(selfKey);
	}
	
}
