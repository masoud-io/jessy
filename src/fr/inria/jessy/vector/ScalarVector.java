package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.util.List;


/**
 * 
 * @author pcincilla
 *
 * @param <K>
 */

public class ScalarVector<K> extends Vector<K> implements Externalizable{

	/**
	 * Checks if the input vector is compatible with this vector. WARNING: it is correct only if is called from the very last version 
	 * up to the first
	 */
	
	@Override
	public boolean isCompatible(Vector<K> other) throws NullPointerException {
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		Integer selfValueOnSelfKey = getSelfValue();
		Integer otherValueOnSelfKey = other.getValue(selfKey);
		
		if(otherValueOnSelfKey<=selfValueOnSelfKey){
			return true;
		}
		
		return false;
	}

	@Override
	public boolean isCompatible(CompactVector<K> other)
			throws NullPointerException {
		
		return isCompatible(other);
	}

	@Override
	public boolean isCompatible(List<Vector<K>> otherList)
			throws NullPointerException {
		
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
		
		new Exception("ScalarVector update can't be called using read set end write set. It has to be called with the new version scalar ");
	}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {
		
		new Exception("ScalarVector update can't be called using read set end write set. It has to be called with the new version scalar ");
	}
	
	@Override
	public void update(int lastCommittedVersionNumber) {
		super.setValue(selfKey, lastCommittedVersionNumber);
	}
}
