package fr.inria.jessy.consistency;

import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.VersionVector;

/**
 * Update the committedVTS according to the received piggyback sequence number.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class ParallelSnapshotIsolationApplyPiggyback implements Runnable{

	private static Logger logger = Logger.getLogger(ParallelSnapshotIsolationApplyPiggyback.class);
	
	private PriorityBlockingQueue<ParallelSnapshotIsolationPiggyback> piggybackQueue;
	
	private Object lock=new Object();

	public ParallelSnapshotIsolationApplyPiggyback() {
		piggybackQueue=new PriorityBlockingQueue<ParallelSnapshotIsolationPiggyback>(11, ParallelSnapshotIsolationPiggyback.ParallelSnapshotIsolationPiggybackComparator);
	}

	public void asyncApply(ParallelSnapshotIsolationPiggyback piggyback) {
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

	public void syncApply(ParallelSnapshotIsolationPiggyback piggyback) {
		
		synchronized(piggyback){
			try {
				addToQueue(piggyback);
				if (!piggyback.isApplied())
					piggyback.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void addToQueue(ParallelSnapshotIsolationPiggyback pb){
		piggybackQueue.offer(pb);
		
		synchronized(lock){
			lock.notify();
		}
	}
	
	@Override
	public void run() {
		ParallelSnapshotIsolationPiggyback pb;
		while (true){
			try {
				pb = piggybackQueue.take();
				updateCommittedVTS(pb);
			} catch (InterruptedException e) {
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
	private void updateCommittedVTS(ParallelSnapshotIsolationPiggyback pb) {
		try{


			if (VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName()) > pb.getSequenceNumber()) {
				/*
				 * If it has been applied before, ignore this one.
				 * 
				 * In theory, this if must never occur.
				 */
				if (ConstantPool.logging)
					logger.error("Transaction "+ pb.getTransactionHandler().getId()+ " wants to update "+ pb.getwCoordinatorGroupName()
							+ " : "+ pb.getSequenceNumber()+ " while current sequence number is "+ VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName()));
				return;
			}


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

			if ((VersionVector.committedVTS
					.getValue(pb.getwCoordinatorGroupName()) < pb.getSequenceNumber()- 1) && (piggybackQueue.size()<1000)){
				
				if (ConstantPool.logging)
					logger.error("late sequence: Transaction "
							+ pb.getTransactionHandler().getId() + " wants to update " + pb.getwCoordinatorGroupName()
							+ " : " + pb.getSequenceNumber()+ " while current sequence number is " + VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName()));

				piggybackQueue.offer(pb);

				synchronized(lock){
					lock.wait();
				}

				return;
			}

			/*
			 * Readset is null in case of init transaction. Thus, we need to ignore init transactions for this test.
			 */
			if (pb.getTransactionType()!=TransactionType.INIT_TRANSACTION){
				
				CompactVector<String> startVTS = pb.getReadsetCompactVector();

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

			synchronized (VersionVector.committedVTS) {
				if (VersionVector.committedVTS.getValue(pb.getwCoordinatorGroupName()) > pb.getSequenceNumber()) {
					return;
				}
				VersionVector.committedVTS.setVector(pb.getwCoordinatorGroupName(),
						(int)pb.getSequenceNumber());

				VersionVector.committedVTS.notifyAll();
			}

			synchronized(pb){
				pb.setApplied(true);
				pb.notify();
			}
			
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}

}
