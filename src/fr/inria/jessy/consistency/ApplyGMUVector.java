package fr.inria.jessy.consistency;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.vector.GMUVector;

public class ApplyGMUVector implements Runnable{

	@SuppressWarnings("deprecation")
	private GMUVector<String> dummyVector=new GMUVector<String>();
	public static ConcurrentHashMap<Integer, GMUVector<String>> toBeAppliedVectors=new ConcurrentHashMap<Integer, GMUVector<String>>();
	
	public void applyCommittedGMUVector(Integer seqNo, GMUVector<String> vector){
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
			GMUVector<String> commitVC=toBeAppliedVectors.remove(appliedSeq++);
			if (commitVC.getMap().size()==0){
				//this is dummyvector because of abort transaction. continue!
				continue;
			}
			if (GMUVector.logCommitVC.size()==ConstantPool.GMUVECTOR_LOGCOMMITVC_SIZE)
				GMUVector.logCommitVC.removeLast();

			/*
			 * If it is the first, we simply add, otherwise, we get the first, and update it with ours, and then put it back.
			 * In any case, the old version of object should be removed.
			 * Old version of object has a key starts with {@link GMUVector.versionPrefix}
			 */
			if (GMUVector.logCommitVC.size()>0){
				GMUVector<String> finalVector;
				finalVector=GMUVector.logCommitVC.getFirst().clone();
				finalVector.updateAndRemove(commitVC, GMUVector.versionPrefix);				
				GMUVector.logCommitVC.addFirst(finalVector);
//				System.out.println("last commitVC is " + finalVector);
			}
			else{
				for (Entry<String,Integer> entry:commitVC.getMap().entrySet()){
					if (entry.getKey().startsWith(GMUVector.versionPrefix))
						commitVC.getMap().remove(entry.getKey());
				}
				GMUVector.logCommitVC.addFirst(commitVC);
//				System.out.println("last commitVC is " + commitVC);
			}
		}
	}
	
}
