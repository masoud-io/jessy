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
	
	/**
	 * needed by BerkleyDB 
	 */
	@SuppressWarnings("unchecked")
	public ScalarVector() {
		super((K) "k");
		super.setValue(selfKey, lastCommittedTransactionSeqNumber.get());
	}

	/**
	 * Checks if the input vector is compatible with this vector. WARNING: it is correct only if is called from the very last version 
	 * up to the first
	 */
	@Override
	public CompatibleResult isCompatible(Vector<K> other) throws NullPointerException {	
		return check(other);
	}

	/**
	 * Checks if the input vector is compatible with this vector. WARNING: it is correct only if is called from the very last version 
	 * up to the first
	 */
	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {

		if (other.size()==0){
			/**
			 * In order to have a snapshot we must know the committedTransactionSeqNumber at witch a transaction start (its snapshot).
			 * Previously this information was keeped on the {@link TransactionHandler}. After Pierre have remove 
			 * it we take lastCommittedTransactionSeqNumber from the first read. For this reason we promote the vector 
			 * of the first read to the lastCommittedTransactionSeqNumber  
			 */
			this.update(lastCommittedTransactionSeqNumber.get());
			return CompatibleResult.COMPATIBLE;
		}
		else{
			return check(other);
		}

	}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {
		
		new Exception("update(CompactVector<K> readSet, CompactVector<K> writeSet) called in ScalarVector, system will exit");
	}
	
	public void update(int newValue){
		super.setValue(selfKey, newValue);
	}
	
	@SuppressWarnings("unchecked")
	private CompatibleResult check(ValueVector other) {
		
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		Integer otherValue = (Integer) other.getValue(selfKey);
		
		/**
		 * this site has not received yet the required version
		 */
		if(otherValue>lastCommittedTransactionSeqNumber.get()){
			return CompatibleResult.NEVER_COMPATIBLE;
		}
		
		Integer selfValue = getValue(selfKey);
		
		
		if(selfValue<=otherValue){ 
			return CompatibleResult.COMPATIBLE;
		}
		
		return CompatibleResult.NOT_COMPATIBLE;
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
