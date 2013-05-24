package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.persist.model.Persistent;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.persistence.FilePersistence;
import fr.inria.jessy.protocol.ApplyGMUVector2;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

/**
 * Used for SRDS submission
 * 
 * Used with NMSI and US with GC
 * 
 * Requires a Vector of size of number of groups!!!
 * 
 * @author Masoud Saeida Ardekani This class implements Vector used in
 *         [Peluso2012].
 * 
 */

@Persistent
public class GMUVector2<K> extends Vector<K> implements Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;

	/**
	 * Defined and used in line 21 of Algorithm 4.
	 */
	public static AtomicInteger lastPrepSC;

	public static LinkedBlockingDeque<GMUVector2<String>> logCommitVC;
	
	public static GMUVector2<String> mostRecentVC;
	
	private static JessyGroupManager manager;
	
	public static final String versionPrefix="user";

	public synchronized static void init(JessyGroupManager m){
		if(lastPrepSC!=null)
			return;
		manager=m;
		if (FilePersistence.loadFromDisk){
			lastPrepSC=(AtomicInteger)FilePersistence.readObject("GMUVector.lastPrepSC");
			mostRecentVC=(GMUVector2<String>) FilePersistence.readObject("GMUVector.mostRecentVC");
		}
		else
		{
			lastPrepSC = new AtomicInteger(0);
			mostRecentVC = new GMUVector2<String>(m.getMyGroup().name(), 0);
		}
		
		logCommitVC=  new LinkedBlockingDeque<GMUVector2<String>>(ConstantPool.GMUVECTOR_LOGCOMMITVC_SIZE);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean prepareRead(ReadRequest rr){
		String myKey=manager.getMyGroup().name();
		CompactVector<String> other=rr.getReadSet();
		
		if (GMUVector2.logCommitVC.peekFirst()==null){
			return true;
		}
		
		//We have not received all update transaction.
		//We are not sure to read or not, thus we try another replica
		if (other.getValue(myKey)!=null  &&
				other.getValue(myKey)-2 > GMUVector2.logCommitVC.peekFirst().getSelfValue() ){
//			if ((other.getValue(myKey) - GMUVector.logCommitVC.peekFirst().getSelfValue())>100){
//				ApplyGMUVector.appliedSeq++;
//			}
			return false;
		}
		
		List<String> hasRead = rr.getReadSet().getKeys();

		if (hasRead.size() == 0) {
			/*
			 * this is the first read because hasRead is not yet initialize.
			 */
			rr.getReadSet().setMap(GMUVector2.logCommitVC.peekFirst().getMap());
		} else if (!hasRead.contains(myKey)) {
			/*
			 * line 3,4 of Algorithm 2
			 */
			Iterator<GMUVector2<String>> itr=GMUVector2.logCommitVC.iterator();
			GMUVector2<String> vector=null;
			
			boolean found=false;;
			while (itr.hasNext()){
				found=false;
				vector=itr.next();
				for (String index : hasRead) {
					if (vector.getValue(index) <= other.getValue(index)){
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
				if (vector!=null)
					rr.getReadSet().update(vector);
			}
			
		} 
		
		return true;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void postRead(ReadRequest rr, JessyEntity entity){
		try{
			int seqNo=entity.getLocalVector().getValue(manager.getMyGroup().name());
			entity.getLocalVector().setMap((HashMap<String, Integer>) rr.getReadSet().getMap().clone());
			if (seqNo>0)
				entity.getLocalVector().setValue(entity.getKey(), seqNo);
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}

	
	/**
	 * Needed for BerkeleyDB
	 */
	@Deprecated
	public GMUVector2(){
	}
	
	public GMUVector2(K selfKey, Integer value) {
		super(selfKey);
		super.setValue(selfKey, value);
	}

	/**
	 * This implementation corresponds to Algorithm 2 in the paper
	 */
	@Override
	public CompatibleResult isCompatible(CompactVector<K> other)
			throws NullPointerException {

		if (getSelfValue()<= other.getValue(getSelfKey())){
			return CompatibleResult.COMPATIBLE;
		}
		else {
			return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
		}
	
	}

	@Override
	public GMUVector2<K> clone() {
		return (GMUVector2<K>) super.clone();
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
	public void updateExtraObjectInCompactVector(Vector<K> vector,Object object) {
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
