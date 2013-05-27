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
	
	public synchronized static void init(JessyGroupManager m){
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean prepareRead(ReadRequest rr){
		try{
			String myKey=""+manager.getSourceId();
			CompactVector<String> other=rr.getReadSet();


			if (GMUVector.logCommitVC.peekFirst()==null){
				return true;
			}

			GMUVectorExtraObject extraObject=(GMUVectorExtraObject)other.getExtraObject();
			if (extraObject==null){
				rr.temporaryVector=GMUVectorExtraObject.getXactVector(GMUVector.logCommitVC.peekFirst());
				return true;
			}

			//We have not received all update transaction.
			//We are not sure to read or not, thus we try another replica
			if (extraObject!=null  &&
					extraObject.xact.getValue(myKey) > GMUVector.logCommitVC.peekFirst().getValue(myKey)){
				return false;
			}

			if (extraObject.hasReads.size() == 0) {
				/*
				 * this is the first read because hasRead is not yet initialize.
				 */
				rr.temporaryVector=GMUVectorExtraObject.getXactVector(GMUVector.logCommitVC.peekFirst());
			} else if (!extraObject.hasReads.contains(myKey)) {
				/*
				 * line 3,4 of Algorithm 2
				 */
				Iterator<GMUVector<String>> itr=GMUVector.logCommitVC.iterator();
				GMUVector<String> vector=null;

				boolean found=false;;
				while (itr.hasNext()){
					found=false;
					vector=itr.next();

					Iterator<String> hasReadItr=extraObject.hasReads.iterator();
					while (hasReadItr.hasNext()) {
						String index=hasReadItr.next();
						if (vector.getValue(index) <= extraObject.xact.getValue(index)){
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
						rr.temporaryVector=GMUVectorExtraObject.getXactVector(vector);
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
	public static void postRead(ReadRequest rr, JessyEntity entity){
		try{
			//Add has read nodes to the object vector.
			// This is a dirty work, and in updateExtraObjectInCompactVector, we remove them, and put them in extraobject of compact vector
			entity.getLocalVector().update(GMUVectorExtraObject.getHasRead(manager, entity.getKey()));
			if (rr.temporaryVector!=null){
				entity.getLocalVector().update(rr.temporaryVector);
			}
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
//		if (getSelfValue()<= other.getValue(getSelfKey())){
//			return CompatibleResult.COMPATIBLE;
//		}
//		else {
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

	// TODO parameterize 4 correctly
	@Override
	public void updateExtraObjectInCompactVector(Vector<K> vector, Object object) {
		try {
			object=(Object) GMUVectorExtraObject.getGMUVectorExtraObject((GMUVector<String>)vector);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public fr.inria.jessy.vector.Vector.CompatibleResult isCompatible(
			Vector<K> other) throws NullPointerException {
		return null;
	}
}
