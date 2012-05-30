package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.persist.model.Persistent;


/**
 * 
 * @author pcincilla
 *
 * @param <K>
 */


@Persistent
public class ScalarVector<K> extends Vector<K> implements Externalizable{
	
	public static AtomicInteger lastCommittedTransactionSeqNumber = new AtomicInteger(0);
		
	public ScalarVector() {
		super(null);
	}

	/**
	 * Checks if the input vector is compatible with this vector. WARNING: it is correct only if is called from the very last version 
	 * up to the first
	 */
	@Override
	public boolean isCompatible(Vector<K> other) throws NullPointerException {	
		return check(other);
	}

	/**
	 * Checks if the input vector is compatible with this vector. WARNING: it is correct only if is called from the very last version 
	 * up to the first
	 */
	@Override
	public boolean isCompatible(CompactVector<K> other)
			throws NullPointerException {

		return check(other);

	}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {
		
		super.setValue(selfKey, lastCommittedTransactionSeqNumber.get());
	}
	
	@SuppressWarnings("unchecked")
	private boolean check(ValueVector other) {
		
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		Integer selfValue = getValue(selfKey);
		Integer otherValue = (Integer) other.getValue(selfKey);
		
		if(selfValue<=otherValue){ 
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
