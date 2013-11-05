package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.persistence.FilePersistence;

/**
 * 
 * @author Pierpaolo Cincilla
 * @author Masoud Saeida Ardekani
 * 
 * @param <K>
 */

@Persistent
public class ScalarVector<K> extends Vector<K> implements Externalizable {
	
	private static final long serialVersionUID = -ConstantPool.JESSY_MID;
	
	/*
	 * Each committed transaction is assigned a sequence number (already given by {@link this#lastCommittingTransactionSeqNumber} 
	 */
	public static  AtomicInteger lastCommittedTransactionSeqNumber;
	
	/*
	 * Each ab-deliver transaction is assigned a new sequence number and put in this queue until it is committed.
	 * This queue is for the sake of reading consistent snapshots.
	 */
	public static  ConcurrentLinkedQueue<Integer> committingTransactionSeqNumber=new ConcurrentLinkedQueue<Integer>();
	
	@Override
	public synchronized void init(JessyGroupManager m){
		if (FilePersistence.loadFromDisk){
			lastCommittedTransactionSeqNumber=(AtomicInteger)FilePersistence.readObject("ScalarVector.lastCommittedTransactionSeqNumber");
		}
		else{
			lastCommittedTransactionSeqNumber = new AtomicInteger(0);
		}
	}
	
	@Override
	public void makePersistent(){
		FilePersistence.writeObject(ScalarVector.lastCommittedTransactionSeqNumber, "ScalarVector.lastCommittedTransactionSeqNumber");
	}
	
	
	public synchronized static int incrementAndGetLastCommittedSeqNumber(){
		synchronized (lastCommittedTransactionSeqNumber) {
			int result=lastCommittedTransactionSeqNumber.incrementAndGet();
				lastCommittedTransactionSeqNumber.notifyAll();	
				return result;
		}
	}
	
	public static void removeCommittingTransactionSeqNumber(int sequenceNumber){
		synchronized (committingTransactionSeqNumber) {
			committingTransactionSeqNumber.remove(sequenceNumber);
			committingTransactionSeqNumber.notifyAll();
		}
		
	}
	
	
	public static int getLastCommittedSeqNumber(){
		return lastCommittedTransactionSeqNumber.get();
	}

	/**
	 * needed by BerkleyDB
	 */
	@SuppressWarnings("unchecked")
	public ScalarVector() {
		super((K) "k");
		super.setValue(selfKey, lastCommittedTransactionSeqNumber.get());
	}

	/**
	 * Checks if the input vector is compatible with this vector. WARNING: it is
	 * correct only if is called from the very last version up to the first
	 */
	@Override
	public CompatibleResult isCompatible(Vector<K> other)
			throws NullPointerException {
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		Integer otherValue = (Integer) other.getValue(selfKey);

		Integer selfValue = getValue(selfKey);

		if (selfValue <= otherValue) {
			return CompatibleResult.COMPATIBLE;
		}

		return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
	}

	/**
	 * Checks if the input vector is compatible with this vector. WARNING: it is
	 * correct only if is called from the very last version up to the first
	 */
	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {

		if (other.size() == 0) {
			/**
			 * In order to have a snapshot we must know the
			 * committedTransactionSeqNumber at witch a transaction start (its
			 * snapshot). Previously this information was kept on the
			 * {@link TransactionHandler}. After Pierre have remove it we take
			 * lastCommittedTransactionSeqNumber from the first read. For this
			 * reason we promote the vector of the first read to the
			 * lastCommittedTransactionSeqNumber
			 */
			this.update(lastCommittedTransactionSeqNumber.get());
			
			return CompatibleResult.COMPATIBLE;
		} else {

			//FIXME THIS IS A BROKEN CODE. READS ARE NOT CONSISTENT!
			Integer otherValue = (Integer) other.getValue(selfKey);
			Integer selfValue = getValue(selfKey);

			if (selfValue <= otherValue) {
				return CompatibleResult.COMPATIBLE;
			}
			else
				return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
			/**
			 * this site has not received yet the required version
			 */
//			if (otherValue > lastCommittedTransactionSeqNumber.get()) {
//				while (otherValue>lastCommittedTransactionSeqNumber.get()){
//					synchronized (lastCommittedTransactionSeqNumber) {
//						try {
//							lastCommittedTransactionSeqNumber.wait();
//						} catch (InterruptedException e) {
//							e.printStackTrace();
//						}	
//
//					}
//				}
//				return CompatibleResult.NEVER_COMPATIBLE;
//			}
//
//			Integer committingHead=committingTransactionSeqNumber.peek();
//			if (committingHead!=null && committingHead<otherValue) {
//				while (committingHead!=null && committingHead<otherValue){
//					synchronized (committingTransactionSeqNumber) {
//						try {
//							committingTransactionSeqNumber.wait(100);
//							committingHead=committingTransactionSeqNumber.peek();
//						} catch (InterruptedException e) {
//							e.printStackTrace();
//						}	
//
//					}
//				}
//				return CompatibleResult.NEVER_COMPATIBLE;
//			}
//			
//			Integer selfValue = getValue(selfKey);
//
//			if (selfValue <= otherValue) {
//				return CompatibleResult.COMPATIBLE;
//			}
//
//			return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
		}

	}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {

		new Exception(
				"update(CompactVector<K> readSet, CompactVector<K> writeSet) called in ScalarVector, system will exit");
	}

	public void update(int newValue) {
		super.setValue(selfKey, newValue);
	}

	@Override
	public ScalarVector<K> clone() {
		return (ScalarVector<K>) super.clone();
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}

}
