package fr.inria.jessy.protocol;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.vector.GMUVector2;

/**
 * Used for SRDS submission
 * 
 * Used with NMSI and US with GC 
 * 
 *  
 * @author msaeida
 *
 */
public class ApplyGMUVector2 implements Runnable{

	@SuppressWarnings("deprecation")
	private GMUVector2<String> dummyVector=new GMUVector2<String>();
	public static ConcurrentHashMap<Integer, GMUVector2<String>> toBeAppliedVectors=new ConcurrentHashMap<Integer, GMUVector2<String>>();
	
	public void applyCommittedGMUVector(Integer seqNo, GMUVector2<String> vector){
		toBeAppliedVectors.put(seqNo, vector);
	}
	
	public void applyAbortedGMUVector(Integer seqNo){
		toBeAppliedVectors.put(seqNo, dummyVector);
		synchronized(toBeAppliedVectors){
			toBeAppliedVectors.notify();
		}
	}
	
	public void GMUVectorIsAdded()	{
		synchronized(toBeAppliedVectors){
			toBeAppliedVectors.notify();
		}
	}
	
	@Override
	public void run() {
		Integer appliedSeq=1;
		Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
		while (true){
			while (!toBeAppliedVectors.containsKey(appliedSeq)){
				synchronized(toBeAppliedVectors){
					try {
						toBeAppliedVectors.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			GMUVector2<String> commitVC=toBeAppliedVectors.remove(appliedSeq++);
			if (commitVC.getMap().size()==0){
				//this is dummyvector because of abort transaction. continue!
				continue;
			}
			if (GMUVector2.logCommitVC.size()==ConstantPool.GMUVECTOR_LOGCOMMITVC_SIZE)
				GMUVector2.logCommitVC.removeLast();

			/*
			 * If it is the first, we simply add, otherwise, we get the first, and update it with ours, and then put it back.
			 * In any case, the old version of object should be removed.
			 * Old version of object has a key starts with {@link GMUVector.versionPrefix}
			 */
			try{
				if (GMUVector2.logCommitVC.size()>0){
					GMUVector2<String> finalVector;
					finalVector=GMUVector2.logCommitVC.getFirst().clone();
					finalVector.updateAndRemove(commitVC, GMUVector2.versionPrefix);
					Iterator<String> itr=finalVector.getMap().keySet().iterator();
					while (itr.hasNext()){
						if (itr.next().startsWith(GMUVector2.versionPrefix))
							itr.remove();
					}
					GMUVector2.logCommitVC.addFirst(finalVector);
				}
				else{
					Iterator<String> itr=commitVC.getMap().keySet().iterator();
					while (itr.hasNext()){
						if (itr.next().startsWith(GMUVector2.versionPrefix))
							itr.remove();
					}
					
					GMUVector2.logCommitVC.addFirst(commitVC);
				}
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
			
		}
	}
	
}
