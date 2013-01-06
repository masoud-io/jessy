package fr.inria.jessy.consistency;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.vector.VersionVector;

import java.util.Comparator;

/**
 * Used in the {@code Vote} for sending the new sequence number to the write
 * set of the transaction.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class ParallelSnapshotIsolationPiggyback implements Externalizable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	
	/**
	 * The group name of the jessy instances that replicate the first write
	 * in the transaction
	 */
	public String wCoordinatorGroupName;

	/**
	 * Incremented sequenceNumber of the jessy instance of the group
	 * {@code wCoordinatorGroupName}
	 */
	public Integer sequenceNumber;

	public ExecutionHistory executionHistory;
	
	/**
	 * This object is set to true if this piggyback is applied to {@link VersionVector#committedVTS}  
	 * 
	 */
	public boolean isApplied =false;

	@Deprecated
	public ParallelSnapshotIsolationPiggyback() {
	}

	public ParallelSnapshotIsolationPiggyback(String wCoordinatorGroupName,
			Integer sequenceNumber, ExecutionHistory executionHistory) {
		this.wCoordinatorGroupName = wCoordinatorGroupName;
		this.sequenceNumber = sequenceNumber;
		this.executionHistory = executionHistory;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		wCoordinatorGroupName = (String) in.readObject();
		sequenceNumber = (Integer) in.readObject();
		executionHistory = (ExecutionHistory) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(wCoordinatorGroupName);
		out.writeObject(sequenceNumber);
		out.writeObject(executionHistory);
	}

	
	public static Comparator<ParallelSnapshotIsolationPiggyback> ParallelSnapshotIsolationPiggybackComparator=new Comparator<ParallelSnapshotIsolationPiggyback>(){

		@Override
		public int compare(ParallelSnapshotIsolationPiggyback o1,
				ParallelSnapshotIsolationPiggyback o2) {
			
			return o1.sequenceNumber.compareTo(o2.sequenceNumber);
		}
		
	};

}
