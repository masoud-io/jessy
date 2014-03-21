package fr.inria.jessy.protocol;

import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.VersionVector;

/**
 * Update the committedVTS according to the received piggyback sequence number.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class VersionVectorApplyPiggyback implements Runnable{

	private static Logger logger = Logger.getLogger(VersionVectorApplyPiggyback.class);
	
	private PriorityBlockingQueue<VersionVectorPiggyback> piggybackQueue;
	
	public VersionVectorApplyPiggyback() {
		piggybackQueue=new PriorityBlockingQueue<VersionVectorPiggyback>(11, VersionVectorPiggyback.ParallelSnapshotIsolationPiggybackComparator);
	}

	public void asyncApply(VersionVectorPiggyback piggyback) {
		/*
		 * If group size is greater than 1, then it can be the case where this node 
		 * has already received a piggyback from another member of the group.
		 * Thus, we can ignore this one. 
		 */
		if (VersionVector.committedVTS.getValue(piggyback.getwCoordinatorGroupName()) >= piggyback.getSequenceNumber()) {
			return;
		}
		
		addToQueue(piggyback);
	}

	public void syncApply(VersionVectorPiggyback piggyback) {
		if (VersionVector.committedVTS.getValue(piggyback.getwCoordinatorGroupName()) > piggyback.getSequenceNumber()) {
			/*
			 * If it has been applied before, ignore this one.
			 * 
			 * In theory, this if must never occur.
			 */
			if (ConstantPool.logging)
				logger.error("Transaction "+ piggyback.getTransactionHandler().getId()+ " wants to update "+ piggyback.getwCoordinatorGroupName()
						+ " : "+ piggyback.getSequenceNumber()+ " while current sequence number is "+ VersionVector.committedVTS.getValue(piggyback.getwCoordinatorGroupName()));
			return;
		}

		synchronized(piggyback){
			try {
				addToQueue(piggyback);
				while (!piggyback.isApplied())
					piggyback.wait();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private void addToQueue(VersionVectorPiggyback pb){
		piggybackQueue.offer(pb);
		
		synchronized(piggybackQueue){
			piggybackQueue.notify();
		}
	}
	
	@Override
	public void run() {
		VersionVectorPiggyback pb;
		while (true){
			try {
				pb = piggybackQueue.take();
				updateCommittedVTS(pb);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Waits until the its conditions hold true, and then update the
	 * committedVTS according to the received sequence number.
	 * 
	 * @param pb
	 *            Received piggyback that contains the group name and its new
	 *            sequence number
	 * @param executionHistory
	 * 
	 *            TODO waiting on committedVTS might not be SAFE. Talk with
	 *            Pierre about it!
	 */
	private void updateCommittedVTS(VersionVectorPiggyback pb) {
		try{


			/*
			 * Two conditions should be held before applying the updates. Figure 13
			 * of [Serrano2011]
			 * 
			 * 1) committedVTS should be greater or equal to startVTS (in order to
			 * ensure causality)
			 * 
			 * 2)committedVTS[group]>sequenceNumber (in order to ensure that all
			 * transactions are serially applied)
			 */
			if (pb.getSequenceNumber() - VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName())> 50){
				/*
				 * This is unsafe. If a timeout happens, the previous sequence number should be increased. 
				 * Since this is not the case currently, we do this.
				 * If there are more than 50 transactions waiting, it means that something wrong. 
				 * To prevent blocking, we should increament by one. 
				 */
				int tmp=VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName());
				VersionVector.committedVTS.setVector(pb.getwCoordinatorGroupName(), tmp+1 );
			}

			/*
			 * We can continue if committedVTS(proxy) < pb-1
			 * or
			 * if committedVTS(proxy)==-1 (meaning there is no entry, and -1 is the default value) and ob==1 (meaning it is the first transaction)
			 */
			if ((VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName()) < pb.getSequenceNumber()- 1)
					&& 
				!(VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName())==-1 && pb.getSequenceNumber()==1))
			{
				
				if (ConstantPool.logging)
					logger.error("late sequence: Transaction "
							+ pb.getTransactionHandler().getId() + " wants to update " + pb.getwCoordinatorGroupName()
							+ " : " + pb.getSequenceNumber()+ " while current sequence number is " + VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName()));


				synchronized(piggybackQueue){
//					System.out.println("Cannot apply " + pb.getwCoordinatorGroupName() + " with " + pb.getSequenceNumber() + " because current seqNO is "  + VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName()) );
					piggybackQueue.wait();
				}
				piggybackQueue.offer(pb);

				return;
			}

			/*
			 * Readset is null in case of init transaction. Thus, we need to ignore init transactions for this test.
			 */
			if (pb.getTransactionType()!=TransactionType.INIT_TRANSACTION){
				
				VersionVector<String> startVTS = (VersionVector<String>) pb.getReadsetCompactVector().getExtraObject();

				if (VersionVector.committedVTS.compareTo(startVTS) < 0	) {
					/*
					 * We have to wait until committedVTS becomes updated through
					 * propagation.
					 */
					if (ConstantPool.logging)
						logger.error("**** Transaction "
								+ pb.getTransactionHandler().getId() + " wants to update " + pb.getwCoordinatorGroupName()
								+ " : " + pb.getSequenceNumber()+ " while current sequence number is " + VersionVector.committedVTS.getValue(pb. getwCoordinatorGroupName()));

					piggybackQueue.offer(pb);

					synchronized(VersionVector.committedVTS){
						VersionVector.committedVTS.wait();
					}

					return;

				}
			}

			if (VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName()) > pb.getSequenceNumber()) {
				setAndNotifyParallelSnapshotIsolationPiggyback(pb);
				return;
			}
			
			synchronized (VersionVector.committedVTS) {
				VersionVector.committedVTS.setVector(pb.getwCoordinatorGroupName(),
						(int)pb.getSequenceNumber());

				VersionVector.committedVTS.notifyAll();
			}

			setAndNotifyParallelSnapshotIsolationPiggyback(pb);
			
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	private void setAndNotifyParallelSnapshotIsolationPiggyback(VersionVectorPiggyback pb){
		synchronized (pb){
			pb.setApplied(true);
			pb.notify();
		}
	}

}
