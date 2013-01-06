package fr.inria.jessy.consistency;

import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory;
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
		addToQueue(piggyback);
	}

	public void syncApply(ParallelSnapshotIsolationPiggyback piggyback) {
		
		synchronized(piggyback){
			try {
				addToQueue(piggyback);
				if (!piggyback.isApplied)
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


			if (VersionVector.committedVTS.getValue(pb.wCoordinatorGroupName) > pb.sequenceNumber) {
				/*
				 * If it has been applied before, ignore this one.
				 * 
				 * In theory, this if must never occur.
				 */
				logger.error("Transaction "+ pb.executionHistory.getTransactionHandler().getId()+ " wants to update "+ pb.wCoordinatorGroupName
						+ " : "+ pb.sequenceNumber+ " while current sequence number is "+ VersionVector.committedVTS.getValue(pb.wCoordinatorGroupName));
				return;
			}

			ExecutionHistory executionHistory = pb.executionHistory;

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
			CompactVector<String> startVTS = executionHistory.getReadSet()
					.getCompactVector();

			if (VersionVector.committedVTS
					.getValue(pb.wCoordinatorGroupName) < pb.sequenceNumber - 1){
				
				if (ConstantPool.logging)
					logger.error("late sequence: Transaction "
							+ pb.executionHistory.getTransactionHandler().getId() + " wants to update " + pb.wCoordinatorGroupName
							+ " : " + pb.sequenceNumber + " while current sequence number is " + VersionVector.committedVTS.getValue(pb.wCoordinatorGroupName));

				piggybackQueue.offer(pb);

				synchronized(lock){
					lock.wait();
				}

				return;
			}

			if (VersionVector.committedVTS.compareTo(startVTS) < 0	) {
				/*
				 * We have to wait until committedVTS becomes updated through
				 * propagation.
				 */
				if (ConstantPool.logging)
					logger.error("**** Transaction "
							+ pb.executionHistory.getTransactionHandler().getId() + " wants to update " + pb.wCoordinatorGroupName
							+ " : " + pb.sequenceNumber + " while current sequence number is " + VersionVector.committedVTS.getValue(pb.wCoordinatorGroupName));

				piggybackQueue.offer(pb);

				synchronized(VersionVector.committedVTS){
					VersionVector.committedVTS.wait();
				}

				return;

			}

			synchronized (VersionVector.committedVTS) {
				if (VersionVector.committedVTS.getValue(pb.wCoordinatorGroupName) > pb.sequenceNumber) {
					return;
				}
				VersionVector.committedVTS.setVector(pb.wCoordinatorGroupName,
						(int)pb.sequenceNumber);

				VersionVector.committedVTS.notifyAll();
			}

			synchronized(pb){
				pb.isApplied=true;
				pb.notify();
			}
			
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}

}
