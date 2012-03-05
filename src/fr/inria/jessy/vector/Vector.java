package fr.inria.jessy.vector;

import java.util.List;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.vector.ValueVector;;

@Persistent
public abstract class Vector<K> extends ValueVector<K,Integer>{

	K selfKey;
	
	public Vector(){
		super(-1);		
	}
	
	public Vector(K selfKey) {
		super(-1);
		this.selfKey = selfKey;
	}
	
	public abstract  boolean isCompatible(Vector<K> other) throws NullPointerException;
	
	public abstract  boolean isCompatible(CompactVector<K> other) throws NullPointerException;

	public abstract boolean isCompatible(List<Vector<K>> otherList)
			throws NullPointerException;

	public abstract void update(List<Vector<K>> readList, List<Vector<K>> writeList);
	
	public abstract void update(CompactVector<K> readSet, CompactVector<K> writeSet);
	
	public K getSelfKey() {
		return selfKey;
	}
	
	public void setSelfKey(K selfKey) {
		this.selfKey = selfKey;
	}

	public Integer getSelfValue() {
		return super.getValue(selfKey);
	}
	
	public Vector<K> clone(){		
		Vector<K> result= (Vector<K>) super.clone();
		result.selfKey=selfKey;
		
		return result;
	}
	
	
}
