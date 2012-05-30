package fr.inria.jessy.vector;

import java.io.Externalizable;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.Jessy;


/**
 * A classical version vector. Used by PSI
 * 
 * @author pcincilla
 *
 * @param <K>
 */
@Persistent
public class VersionVector<K> extends Vector<K> implements Externalizable {

	
	
	public VersionVector(K selfKey) {
		super(selfKey);
		super.setValue(selfKey, 0);
	}
	
	
	/**
	 * Checks if the input vector is compatible with this vector. WARNING: it is correct only if is called from the very last version 
	 * up to the first
	 */
	@Override
	public boolean isCompatible(Vector<K> other) throws NullPointerException {
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		Integer selfValueOnOtherKey = getValue(other.getSelfKey());
		Integer otherValueOnOtherKey = other.getSelfValue();
		
		return compareKeys(selfValueOnOtherKey, otherValueOnOtherKey);

	}

	@Override
	public boolean isCompatible(CompactVector<K> other)
			throws NullPointerException {
		
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

//		TODO: check; only one key because is per-site and not per-object?!? 
		
		K k=other.getKeys().iterator().next();
		
		Integer selfValueOnOtherKey = getValue(k);
		Integer otherValueOnOtherKey = other.getValue(k);
		
		return compareKeys(selfValueOnOtherKey, otherValueOnOtherKey);
	}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {
		 
		super.setValue(selfKey, ScalarVector.lastCommittedTransactionSeqNumber.get());
		
	}
	
	private boolean compareKeys(Integer selfValueOnOtherKey, Integer otherValueOnOtherKey) {

		if (selfValueOnOtherKey<=otherValueOnOtherKey) {
			return true;
		}
		return false;
	}

}
