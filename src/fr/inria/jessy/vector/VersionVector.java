package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.util.Iterator;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.persistence.FilePersistence;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

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
	
	@Override
	public synchronized void init(JessyGroupManager m){
		
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
		
		K key= (K)committedVTS.getSelfKey();

		if ((this.getValue(key)== 0 || this.getValue(key)== -1)
				&& (other.getValue(key)== 0 || other.getValue(key)== -1))
		{
			/*
			 * This test is for objects with initial version. 
			 * For example consider object user1 is replicated at two groups g1 and g2.
			 * Hence their initial versions are <g1,0> and <g2,0> accordingly.
			 * Thus if proxy reads from g2, and the transaction coordinator is g1 (in 2pc), 
			 * then the two versions are different.
			 * In other words, when g1 reads the last committed version of user1, it return <g1,0>, 
			 * but the last read return <g1,-1> because g1 does not exist in user1 version. 
			 * and -1 is the default return value.  
			 */

			return Vector.CompatibleResult.COMPATIBLE;
		}
		
		
		if (this.getValue(key) >= other.getValue(key))
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
			return Vector.CompatibleResult.COMPATIBLE;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public boolean prepareRead(ReadRequest rr){
		K key= (K)committedVTS.getSelfKey();
		try{
			VersionVector<K> vector=(VersionVector<K>) rr.getReadSet().getExtraObject();
			if (vector==null || vector.size()==0){
				//first read. we go ahead and read.
				return true;
			}			
			else{
				if (getValue(key) <= vector.getValue(key))
					return true;
				else
					return false;
			}
			
					
		}
		catch(Exception ex){
			ex.printStackTrace();
		}

		return false;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void postRead(ReadRequest rr, JessyEntity entity){
		
		try{
			if (rr.getReadSet().size()==0){
				//this is the first read.
				//we need to set the transaction snapshot. 
				//we set the vector of the node as a temprory object.
				//Later, once the proxy gets the answer, it needs to incorporate it into the compactVector extra object.
				VersionVector<String> vector=new VersionVector<String>();
				Iterator<String> itr = committedVTS.getVector().keySet().iterator();
				while (itr.hasNext()) {
					String key = itr.next();
					int value = committedVTS.getValue(key);
					vector.setValue(key, value);
				}
				
				entity.temporaryObject=vector;
			}
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	@Override
	public void updateExtraObjectInCompactVector(Vector<K> entityLocalVector, Object entityTemproryObject, ExtraObjectContainer compactVectorExtraObjectContainer) {
		if (entityTemproryObject!=null){
			//The first read has been done, and we need to set the transaction snapshot. 
			//We do this by setting the extra object in compact vector. 
			compactVectorExtraObjectContainer.extraObject  =((VersionVector<String>)entityTemproryObject).clone();
		}
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

	@Override
	public void makePersistent(){
		FilePersistence.writeObject(VersionVector.committedVTS, "VersionVector.committedVTS");
	}

}
