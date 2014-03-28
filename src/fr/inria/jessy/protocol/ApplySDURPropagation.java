package fr.inria.jessy.protocol;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.VersionVector;

public class ApplySDURPropagation extends Thread{
	
	BlockingQueue<String> pendings;
	
	ConcurrentHashMap<String, SDURPiggybackContainer> containers;
	
	public ApplySDURPropagation(){
		pendings=new LinkedBlockingQueue<String>();
		containers=new ConcurrentHashMap<String, ApplySDURPropagation.SDURPiggybackContainer>();
		
		this.start();
	}
	
	
	/**
	 * this should be called once a transaction is ready to commit. 
	 */
	public synchronized void createContainer(TransactionHandler handler, Set<String> requiredPiggybacks, SDURPiggyback selfPiggyback){
		try{

			pendings.add(handler.getId().toString());
			SDURPiggybackContainer exist=containers.putIfAbsent(handler.getId().toString(), new SDURPiggybackContainer(requiredPiggybacks,selfPiggyback));
			if (exist!=null){
				//already received a piggyback. we need to update it
				exist.requiredDest=requiredPiggybacks;
				exist.receivedPiggybacks.add(selfPiggyback);
			}
			notifyContainers();
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
	}
	
	/**
	 * this should be called 
	 */
	public synchronized void addReceivedPiggyback(TransactionHandler handler, SDURPiggyback pb){
		try{

			SDURPiggybackContainer exist=containers.putIfAbsent(handler.getId().toString(), new SDURPiggybackContainer(pb));
			if (exist!=null){
				//already received a piggyback. we need to update it
				exist.receivedPiggybacks.add(pb);
			}
			notifyContainers();
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
	}
	
	private void notifyContainers(){
		synchronized(containers){
			containers.notifyAll();
		}
	}

	public void run() {
		try{
			while (true){
				String UUIDhead=pendings.take().toString();
				while (!containers.containsKey(UUIDhead) || !containers.get(UUIDhead).canProceed(UUIDhead)){
					synchronized (containers){
						containers.wait();
					}
				}

				//We have received all piggybacks. We can update the commitVTS
				Set<SDURPiggyback> receivedPiggybacks=containers.get(UUIDhead).receivedPiggybacks;
				for (SDURPiggyback pb: receivedPiggybacks){
					if (VersionVector.committedVTS.getValue(pb.getGroupName()) < pb.getSC()){
						VersionVector.committedVTS.setVector(pb.getGroupName(), pb.getSC());
					}
				}
				
				//Done, we can clean up now.
				containers.remove(UUIDhead);
			}
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		
	}

	private class SDURPiggybackContainer{
		Set<SDURPiggyback> receivedPiggybacks;
		Set<String> requiredDest; 
		
		 SDURPiggybackContainer(Set<String> requiredPiggybacks, SDURPiggyback selfPiggyback){
			this.requiredDest=requiredPiggybacks;
			receivedPiggybacks=new HashSet<SDURPiggyback>();
			receivedPiggybacks.add(selfPiggyback);
		}
		
		 SDURPiggybackContainer(SDURPiggyback pb){
			receivedPiggybacks=new HashSet<SDURPiggyback>();
			receivedPiggybacks.add(pb);
		 }

		 boolean canProceed(String id){
			 if (receivedPiggybacks==null || requiredDest==null)
			 {
				 return false;
			 }
			 return  receivedPiggybacks.size()==requiredDest.size();
		 }

	}
}


