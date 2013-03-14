package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.util.Iterator;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.persistence.FilePersistence;

/**
 * A classical version vector. To be used by PSI.
 * 
 * Each object version holds a Vector. Moreover, there exists a static
 * ValueVector called {@code observedCommittedTransactions}. SelfKey will be
 * used in this class as the index of the group of jessy instances that plays
 * the role of the transaction coordinator. E.g., Consider jessy instances p1
 * and p2 replicate entity x, and belongs to group g1. If transaction T1
 * modifies entity x, the the selfkey of committed version of entity x is g1.
 * 
 * @author Masoud Saeida Ardekani
 * 
 * @param <K>
 */
@Persistent
public class VersionVector<K> extends Vector<K> implements Cloneable, Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;
	
	/**
	 * this Vector plays the role of a vector assigned to each jessy server in
	 * the system.
	 */
	public static ConcurrentVersionVector<String> committedVTS;
	
	@Deprecated
	public VersionVector(){}
	
	
	public VersionVector(JessyGroupManager m) {
		init(m);
	}
	
	// FIXME ugly construct
	public synchronized static void init(JessyGroupManager m){
		
		if(committedVTS!=null) return;
		
		if (FilePersistence.loadFromDisk)
			committedVTS= (ConcurrentVersionVector<String>) FilePersistence.readObject("VersionVector.committedVTS");
		else
			committedVTS = new ConcurrentVersionVector<String>(m.getMyGroup().name());
		
	}

	public VersionVector(K selfKey, Integer selfValue) {
		super(selfKey);
		super.setValue(selfKey, selfValue);
	}

	/**
	 * @inheritDoc
	 * 
	 *             WARNING: it is correct only if is called from the very last
	 *             version up to the first.
	 */
	@Override
	public CompatibleResult isCompatible(Vector<K> other)
			throws NullPointerException {
		if (other == null) {
			throw new NullPointerException("Input Vector is Null");
		}

		if (this.getValue(selfKey).compareTo(other.getValue(selfKey)) <= 0)
			return Vector.CompatibleResult.COMPATIBLE;
		else
			return Vector.CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;

	}

	/**
	 * @inheritDoc
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {
		if (other == null)
			throw new NullPointerException("Input Vector is Null");

		if (other.size() == 0) {
			/*
			 * if other vector size is zero, then this is the very first read.
			 * Thus, we set the StartVTS
			 * 
			 * Notice that we cannot simply set the committedVTS to local vector
			 * because we need a clone of it, and ConcurrentHashMap does not
			 * have a clone implementation.
			 */
			Iterator<String> itr = committedVTS.getVector().keySet().iterator();
			while (itr.hasNext()) {
				String key = itr.next();
				int value = committedVTS.getValue(key);
				this.setValue((K) key, (Integer) value);
			}

			return Vector.CompatibleResult.COMPATIBLE;
		}

		if (getValue(selfKey) <= other.getValue(selfKey))
			return Vector.CompatibleResult.COMPATIBLE;
		else
			return Vector.CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {
		// Readset can be simply applied by using the update method of
		// ValueVector Class
		if (readSet.size() > 0)
			super.update(readSet);

	}
	
	public VersionVector<K> clone() {
		return (VersionVector<K>) super.clone();

	}


}
