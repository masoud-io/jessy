package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.Jessy;


/**
 * 
 * @author pcincilla
 *
 * @param <K>
 */

@Persistent
public class ScalarVector<K> extends Vector<K> implements Externalizable{
	
	public ScalarVector() {
		super();
	}
			
//	public ScalarVector(K selfKey) {
//		super(selfKey);
//		super.setValue(selfKey, 0);
//	}

	/**
	 * Checks if the input vector is compatible with this vector. WARNING: it is correct only if is called from the very last version 
	 * up to the first
	 */
	
	@Override
	public boolean isCompatible(Vector<K> other) throws NullPointerException {
		
		return check(other);
	}

	@Override
	public boolean isCompatible(CompactVector<K> other)
			throws NullPointerException {

		return check(other);

	}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {
		
//		new Exception("ScalarVector update can't be called using read set end write set. It has to be called with the new version scalar ");
		super.setValue(selfKey, Jessy.lastCommittedTransactionSeqNumber.get());
	}
	
	@SuppressWarnings("unchecked")
	private boolean check(ValueVector other) {
		
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		Integer selfValueOnSelfKey = getValue(selfKey);
		Integer otherValueOnSelfKey = (Integer) other.getValue(selfKey);
		
//		K otherKey = (K) ((ScalarVector)other).getSelfKey();
//		Integer otherValueOnSelfKey = (Integer) other.getValue(otherKey);
		
		if(selfValueOnSelfKey<=otherValueOnSelfKey){ 
			return true;
		}
		
		return false;
	}
	
	@Override
	public ScalarVector<K> clone() {
		return (ScalarVector<K>) super.clone();
	}
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}
}
