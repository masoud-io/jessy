package fr.inria.jessy.vector;

import java.util.List;

/**
 * @author Masoud Saeida Ardekani This class implements dependence vector for
 *         jessy objects.
 */
public class DependenceVector<K> extends Vector<K>{


	public DependenceVector(K selfKey){
		super(selfKey);
	}
 
	@Override
	public boolean isCompatible(
			Vector<K> other) throws NullPointerException {
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
	public boolean isCompatible(
			List<Vector<K>> otherList) throws NullPointerException {
		// check special values
		if (otherList == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		for (Vector<K> other : otherList) {
			if (isCompatible(other) == false)
				return false;
		}
		return true;
	}
	
	@Override
	public void update(List<Vector<K>> readList, List<Vector<K>> writeList) {
		for(Vector<K> readVector:readList){
			super.update(readVector);
		}
		
		for(Vector<K> writeVector:writeList){
			super.update(writeVector);
			super.setValue(writeVector.getSelfKey(), writeVector.getSelfValue()+1);
		}
		
	}



 
}
