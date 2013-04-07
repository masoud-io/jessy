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
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

/**
 * @author Masoud Saeida Ardekani This class implements Vector used in
 *         [Peluso2012].
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
	
	public static GMUVector<String> mostRecentVC;
	
	private static JessyGroupManager manager;
	
	public static final String versionPrefix="*";

	public synchronized static void init(JessyGroupManager m){
		if(lastPrepSC!=null)
			return;
		manager=m;
		if (FilePersistence.loadFromDisk){
			lastPrepSC=(AtomicInteger)FilePersistence.readObject("GMUVector.lastPrepSC");
			mostRecentVC=(GMUVector<String>) FilePersistence.readObject("GMUVector.mostRecentVC");
		}
		else
		{
			lastPrepSC = new AtomicInteger(0);
			mostRecentVC = new GMUVector<String>(m.getMyGroup().name(), 0);
		}
		
		logCommitVC=  new LinkedBlockingDeque<GMUVector<String>>(ConstantPool.GMUVECTOR_LOGCOMMITVC_SIZE);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean prepareRead(ReadRequest rr){
		String myKey=manager.getMyGroup().name();
		CompactVector<String> other=rr.getReadSet();
		
		if (GMUVector.logCommitVC.peekFirst()==null){
			return true;
		}
		
		if (other.getValue(myKey)!=null  &&
				other.getValue(myKey) > GMUVector.logCommitVC.peekFirst().getSelfValue() ){
			//We have not received all update transaction.
			//We are not sure to read or not, thus we try another replica
			return false;
		}
		
		List<String> hasRead = rr.getReadSet().getKeys();

		if (hasRead.size() == 0) {
			/*
			 * this is the first read because hasRead is not yet initialize.
			 */
			rr.getReadSet().setMap(GMUVector.logCommitVC.peekFirst().getMap());
		} else if (!hasRead.contains(myKey)) {
			/*
			 * line 3,4 of Algorithm 2
			 */
			Iterator<GMUVector<String>> itr=GMUVector.logCommitVC.iterator();
			GMUVector<String> vector=null;
			
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
			entity.getLocalVector().setMap(rr.getReadSet().getMap());
			entity.getLocalVector().setValue(GMUVector.versionPrefix+manager.getMyGroup().name(), seqNo);
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

		if (getSelfValue()<= other.getValue(getSelfKey())){
			return CompatibleResult.COMPATIBLE;
		}
		else {
			return CompatibleResult.NOT_COMPATIBLE_TRY_NEXT;
		}
	
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
