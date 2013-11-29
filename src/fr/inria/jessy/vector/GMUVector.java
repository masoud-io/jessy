package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.persistence.FilePersistence;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

/**
 * Require a vector of size of number of processes
 * 
 * @author Masoud Saeida Ardekani This class implements Vector exactly introduced in [Peluso2012].
 * 
 */

@Persistent
public class GMUVector<K> extends Vector<K> implements Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;

	/**
	 * Defined and used in line 21 of Algorithm 4.
	 */
	public static AtomicInteger lastPrepSC;

	public static LinkedBlockingDeque<GMUVector<String>> logCommitVC;
	
	private static JessyGroupManager manager;
	
	@Override
	public synchronized void init(JessyGroupManager m){
		if(lastPrepSC!=null)
			return;
		manager=m;
		if (FilePersistence.loadFromDisk){
			lastPrepSC=(AtomicInteger)FilePersistence.readObject("GMUVector.lastPrepSC");
		}
		else
		{
			lastPrepSC = new AtomicInteger(0);
		}
		
		logCommitVC=  new LinkedBlockingDeque<GMUVector<String>>(ConstantPool.GMUVECTOR_LOGCOMMITVC_SIZE);
	}
	
	@Override
	public void makePersistent(){
		FilePersistence.writeObject(GMUVector.lastPrepSC, "GMUVector.lastPrepSC");
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public boolean prepareRead(ReadRequest rr){
		try{
			String myKey=""+manager.getSourceId();
			CompactVector<String> other=rr.getReadSet();


			if (GMUVector.logCommitVC.peekFirst()==null){
				return true;
			}

			GMUVectorExtraObject extraObject=(GMUVectorExtraObject)other.getExtraObject();
			if (extraObject==null){
				rr.temporaryVector=GMUVector.logCommitVC.peekFirst();
				return true;
			}

			//We have not received all update transaction.
			//We are not sure to read or not, thus we try another replica
			if (extraObject!=null  &&
					extraObject.getSnapshot().getValue(myKey).compareTo(GMUVector.logCommitVC.peekFirst().getValue(myKey)) >=0){
				return false;
			}

			if (extraObject.getReadProcesses().size() == 0) {
				/*
				 * this is the first read because hasRead is not yet initialize.
				 */
				rr.temporaryVector=GMUVector.logCommitVC.peekFirst();
			} else if (!extraObject.getReadProcesses().contains(myKey)) {
				/*
				 * line 3,4 of Algorithm 2
				 */
				Iterator<GMUVector<String>> itr=GMUVector.logCommitVC.iterator();
				GMUVector<String> vector=null;

				boolean found=false;;
				while (itr.hasNext()){
					found=false;
					vector=itr.next();

					Iterator<String> hasReadItr=extraObject.getReadProcesses().iterator();
					while (hasReadItr.hasNext()) {
						String index=hasReadItr.next();
						if (vector.getValue(index) <= ((Integer)extraObject.getSnapshot().getValue(index))){
							found=true;
							continue;
						}
						else{
							break;
						}
					}
					if (found)
						break;
				}
				if (!found){
					return false;
				}
				else{
					if (vector!=null){
						//Add xact vector to the object vector.
						// This is a dirty work, and in updateExtraObjectInCompactVector, we remove them, and put them in extraobject of compact vector
						rr.temporaryVector=vector;
					}
				}

			} 

			return true;
		}
		catch (Exception ex){
			ex.printStackTrace();			
			return false;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void postRead(ReadRequest rr, JessyEntity entity){
		try{
			//we set the vector of the node as a temprory object.
			//Later, once the proxy gets the answer, it needs to incorporate it into the compactVector extra object.
			entity.temporaryObject=rr.temporaryVector;
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}

	
	/**
	 * Needed for BerkeleyDB
	 */
	@Deprecated
	public GMUVector(){
	}
	
	public GMUVector(K selfKey, Integer value) {
		super(selfKey);
		super.setValue(selfKey, value);
	}

	/**
	 * This implementation corresponds to Algorithm 2 in the paper
	 */
	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {

		return CompatibleResult.COMPATIBLE;
//		if (getSelfValue()<= other.getValue(getSelfKey()) || (other.getValue(getSelfKey())==-1)){
//			return CompatibleResult.COMPATIBLE;
//		}
//		else {
//			System.out.println("Cannot read because getSelfValue(): " + getSelfValue() + " and other.getValue(getSelfKey()) " + other.getValue(getSelfKey()) + " and GMUVector.logCommitVC.peekFirst() " + GMUVector.logCommitVC.peekFirst());
//			return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
//		}
	}

	@Override
	public GMUVector<K> clone() {
		return (GMUVector<K>) super.clone();
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
	}

	@Override
	public void updateExtraObjectInCompactVector(Vector<K> entityLocalVector, Object entityTemproryObject, Object compactVectorExtraObject) {
		GMUVectorExtraObject obj=(GMUVectorExtraObject)compactVectorExtraObject;
		if (obj==null)
			obj=new GMUVectorExtraObject();
		GMUVector<K> tmpSnapshot=(GMUVector<K>)entityTemproryObject;
		if (obj.getSnapshot()==null){
			obj.setSnapshot(tmpSnapshot);
		}
		else{
			//we must only update the entry of last partition
			if (tmpSnapshot.getSelfValue() > (Integer)obj.getSnapshot().getValue(tmpSnapshot.getSelfKey()))
				obj.getSnapshot().setValue(tmpSnapshot.getSelfKey(), tmpSnapshot.getSelfValue());
		}
		obj.addItem((GMUVector<K>) entityLocalVector);
	}

	@Override
	public fr.inria.jessy.vector.Vector.CompatibleResult isCompatible(
			Vector<K> other) throws NullPointerException {
		return null;
	}
}
