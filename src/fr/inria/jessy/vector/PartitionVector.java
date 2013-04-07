package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.persistence.FilePersistence;

/**
 * @author Masoud Saeida Ardekani This class implements Vector used in
 *         [Peluso2012].
 * 
 */

@Persistent
public class PartitionVector<K> extends Vector<K> implements Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;

	/**
	 * Defined and used in line 21 of Algorithm 4.
	 */
	public static AtomicInteger lastPrepSC;

	public static PartitionVector<String> mostRecentVC;

	public synchronized static void init(JessyGroupManager m){
		if(lastPrepSC!=null)
			return;
		if (FilePersistence.loadFromDisk){
			lastPrepSC=(AtomicInteger)FilePersistence.readObject("GMUVector.lastPrepSC");
			mostRecentVC=(PartitionVector<String>) FilePersistence.readObject("GMUVector.mostRecentVC");
		}
		else
		{
			lastPrepSC = new AtomicInteger(0);
			mostRecentVC = new PartitionVector<String>(m.getMyGroup().name(), 0);
		}

	}
	
	/**
	 * Needed for BerkeleyDB
	 */
	@Deprecated
	public PartitionVector(){
	}
	
	public PartitionVector(K selfKey, Integer value) {
		super(selfKey);
		super.setValue(selfKey, value);
	}

	/**
	 * This implementation corresponds to Algorithm 2 in the paper
	 */
	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {

		// check special values

		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		if (other.size() == 0)
			return Vector.CompatibleResult.COMPATIBLE;

		
		Integer selfValueOnSelfKey = getSelfValue();
		Integer otherValueOnSelfKey = other.getValue(selfKey);

		/*
		 * Reading from the same partition
		 * We need to return the maximum version less than other
		 */
		if (other.getKeys().get(0)!=null && other.getKeys().get(0).equals(selfKey)){
			System.out.println("Bingo inside the same partition " + getSelfKey() + " my value : " + selfValueOnSelfKey + " other value on my key " + otherValueOnSelfKey);
			if (selfValueOnSelfKey<=otherValueOnSelfKey)
				return CompatibleResult.COMPATIBLE;
			else
				return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
		}

		if (selfValueOnSelfKey < otherValueOnSelfKey) {
			return Vector.CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
		}

		Integer selfValueOnOtherKey;
		Integer otherValueOnOtherKey;

		for (K k : other.getKeys()) {
			selfValueOnOtherKey = getValue(k);
			otherValueOnOtherKey = other.getValue(k);

			if (otherValueOnOtherKey < selfValueOnOtherKey) {
				return Vector.CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
			}

		}

		return Vector.CompatibleResult.COMPATIBLE;
		
//		HashMap<K, Boolean> hasRead = (HashMap<K, Boolean>) other
//				.getExtraObject();
//
//		Integer maxVCAti;
//
//		if (hasRead == null) {
//			/*
//			 * this is the first read because hasRead is not yet initialize.
//			 */
//			maxVCAti = getValue(getSelfKey());
//		} else if (!hasRead.get(getSelfKey())) {
//			/*
//			 * line 3,4 of Algorithm 2
//			 */
//			for (K index : hasRead.keySet()) {
//				if (getValue(index) > other.getValue(index))
//					return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
//			}
//			maxVCAti = getValue(getSelfKey());
//		} else {
//			maxVCAti = other.getValue(getSelfKey());
//		}
//
//		if (getSelfValue() <= maxVCAti)
//			return CompatibleResult.COMPATIBLE;
//		else
//			return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
	}

	@Override
	public PartitionVector<K> clone() {
		return (PartitionVector<K>) super.clone();
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}

	// TODO parameterize 4 correctly
	@Override
	public void updateExtraObjectInCompactVector(Object object) {
		/*
		 * init with 4 entries, because for the moment, we have 4 reads.
		 */
		if (object == null)
			object = new HashMap<K, Boolean>(4);
		((HashMap<K, Boolean>) object).put(selfKey, true);
	}

	@Override
	public fr.inria.jessy.vector.Vector.CompatibleResult isCompatible(
			Vector<K> other) throws NullPointerException {
		return null;
	}
}
