package fr.inria.jessy.vector;

import java.util.List;

/**
 * @author Masoud Saeida Ardekani This class implements dependence vector for
 *         jessy objects.
 */
public class DependenceVector<K> extends ValueVector<K, Integer> implements IVector<K>{

	K selfKey;

	public DependenceVector(K selfKey) {
		super(-1);
		this.selfKey = selfKey;
	}
	
	public K getSelfKey() {
		return selfKey;
	}

	public Integer getSelfValue() {
		return super.getValue(selfKey);
	}
 
	@Override
	public <V extends ValueVector<K, Integer> & IVector<K>> boolean isReadable(
			V other) throws NullPointerException {
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
			return true;
		}

		return false;
	}
 

	@Override
	public <V extends ValueVector<K, Integer> & IVector<K>> boolean isListReadable(
			List<V> otherList) throws NullPointerException {
		// check special values
		if (otherList == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		for (V other : otherList) {
			if (isReadable(other) == false)
				return false;
		}
		return true;
	}
	
	@Override
	public <V extends ValueVector<K, Integer> & IVector<K>> void update(
			List<V> readList, List<V> writeList) {
		for(V readVector:readList){
			super.update(readVector);
		}
		
		for(V writeVector:writeList){
			super.update(writeVector);
			super.setValue(writeVector.getSelfKey(), writeVector.getSelfValue()+1);
		}
		
	}



 
}
