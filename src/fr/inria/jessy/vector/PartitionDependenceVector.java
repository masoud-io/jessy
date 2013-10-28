package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

/**
 * @author Masoud Saeida Ardekani This class implements partition dependence vector for
 *         jessy objects.
 * 
 */

@Persistent
public class PartitionDependenceVector<K> extends Vector<K> implements Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;

	public static PartitionDependenceVector<String> lastCommit=new PartitionDependenceVector<String>();

	public static AtomicInteger lastCommitSeqNo=new AtomicInteger();
	
	/**
	 * Needed for BerkeleyDB
	 */
	@Deprecated
	public PartitionDependenceVector(){
	}
	
	public PartitionDependenceVector(K selfKey, Integer value) {
		super(selfKey);
		super.setValue(selfKey, value);
	}

	/**
	 * This object (that will be called x in the code) should be consistent with all previously read items
	 * that present in {@link CompactVector#getExtraObject()}
	 */
	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {
		PDVExtraObject<K> extraObject=(PDVExtraObject<K>)other.getExtraObject();
		if (extraObject == null || extraObject.getSnapshot()==null){
			//this is the first read, we simply return compatible.
			return CompatibleResult.COMPATIBLE.COMPATIBLE;
		}
		
		CompatibleResult result=CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
		K xParition=this.getSelfKey();		
		K yParition;
		for (PartitionDependenceVector<K> entity: extraObject.getReadItems()){
			yParition=entity.getSelfKey();
			if (xParition.equals(yParition)){
				//test for consistency of two objects inside the same partition
				if (getValue(xParition) > other.getValue(xParition)){
					//if the value of what we want to read is smaller than snapshot, it is safe to read
					//otherwise, we return not compatible to be safe.
					//Note that to be sure, we need to check that the read version is the last version before the write of this object version which is expensive.
					//Hence, we return not compatible to be fast.
					if (extraObject.getSnapshot().getValue(xParition)>=getValue(xParition)){
						result= CompatibleResult.COMPATIBLE.COMPATIBLE;
					}
					else{
						return CompatibleResult.COMPATIBLE.NEVER_COMPATIBLE;
					}
				}else if (getValue(xParition) < other.getValue(xParition)){

					//with two assumptions, it is safe to return compatible here:
					//1. reads on the same partition always go to the same partition
					//2. writes are applied atomically. I.e., once a new version of object x is visible, the new version of object y is also visible given that they are written in the same function.
					result= CompatibleResult.COMPATIBLE.COMPATIBLE;
				}				
			}
			else{
				//test for consistency of two objects that are NOT inside the same partition
				//this is easy, like in dependence vector
				if ((getValue(xParition) > other.getValue(xParition)) && 
						(getValue(yParition) < other.getValue(yParition))){
					result= CompatibleResult.COMPATIBLE.COMPATIBLE;
				}
				else{
					return CompatibleResult.COMPATIBLE.NOT_COMPATIBLE_TRY_NEXT;
				}
			}
			continue;
		}
		return result;
	}
	
	
	@Override
	public fr.inria.jessy.vector.Vector.CompatibleResult isCompatible(
			Vector<K> other) throws NullPointerException {
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean prepareRead(ReadRequest rr){
		rr.temporaryVector=lastCommit;
		return true;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void postRead(ReadRequest rr, JessyEntity entity){
		try{
			//we set the vector of the node as a temprory object.
			//Later, once the proxy gets the answer, it needs to incorporate it into the compactVector extra object.
			entity.temporaryObject=rr.temporaryVector;
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	@Override
	public void updateExtraObjectInCompactVector(Vector<K> entityLocalVector, Object entityTemproryObject, Object compactVectorExtraObject) {
		PDVExtraObject obj=(PDVExtraObject)compactVectorExtraObject;
		if (obj==null)
			obj=new PDVExtraObject<K>();
		PartitionDependenceVector<K> tmpSnapshot=(PartitionDependenceVector<K>)entityTemproryObject;
		if (obj.getSnapshot()==null){
			obj.setSnapshot(tmpSnapshot);
		}
		else{
			//we must only update the entry of last partition
			if (tmpSnapshot.getSelfValue() > (Integer)obj.getSnapshot().getValue(tmpSnapshot.getSelfKey()))
				obj.getSnapshot().setValue(tmpSnapshot.getSelfKey(), tmpSnapshot.getSelfValue());
		}
		obj.addItem((PartitionDependenceVector<K>) entityLocalVector);
	}
	
	@Override
	public PartitionDependenceVector<K> clone() {
		return (PartitionDependenceVector<K>) super.clone();
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}

}
