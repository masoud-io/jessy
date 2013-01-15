package fr.inria.jessy.consistency;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.VersionVector;

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
	private String wCoordinatorGroupName;

	/**
	 * Incremented sequenceNumber of the jessy instance of the group
	 * {@code wCoordinatorGroupName}
	 */
	private Integer sequenceNumber;

	private CompactVector<String> readsetCompactVector;
	
	private TransactionType transactionType;

	private TransactionHandler transactionHandler; 
	
	/**
	 * This object is set to true if this piggyback is applied to {@link VersionVector#committedVTS}  
	 * 
	 */
	private boolean isApplied =false;

	@Deprecated
	public ParallelSnapshotIsolationPiggyback() {
	}

	public ParallelSnapshotIsolationPiggyback(String wCoordinatorGroupName,
			Integer sequenceNumber, ExecutionHistory executionHistory) {
		this.wCoordinatorGroupName = wCoordinatorGroupName;
		this.sequenceNumber = sequenceNumber;
		this.transactionType=executionHistory.getTransactionType();
		this.transactionHandler=executionHistory.getTransactionHandler();

		if (executionHistory.getTransactionType()!=TransactionType.INIT_TRANSACTION){
			this.readsetCompactVector=executionHistory.getReadSet().getCompactVector();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		wCoordinatorGroupName = (String) in.readObject();
		sequenceNumber = (Integer) in.readObject();
		readsetCompactVector = (CompactVector<String>) in.readObject();
		transactionType=(TransactionType) in.readObject();
		transactionHandler=(TransactionHandler)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(wCoordinatorGroupName);
		out.writeObject(sequenceNumber);
		out.writeObject(readsetCompactVector);
		out.writeObject(transactionType);
		out.writeObject(transactionHandler);
	}

	
	public static Comparator<ParallelSnapshotIsolationPiggyback> ParallelSnapshotIsolationPiggybackComparator=new Comparator<ParallelSnapshotIsolationPiggyback>(){

		@Override
		public int compare(ParallelSnapshotIsolationPiggyback o1,
				ParallelSnapshotIsolationPiggyback o2) {
			
			return o1.sequenceNumber.compareTo(o2.sequenceNumber);
		}
		
	};

	public String getwCoordinatorGroupName() {
		return wCoordinatorGroupName;
	}

	public Integer getSequenceNumber() {
		return sequenceNumber;
	}

	public CompactVector<String> getReadsetCompactVector() {
		return readsetCompactVector;
	}

	public TransactionType getTransactionType() {
		return transactionType;
	}

	public boolean isApplied() {
		return isApplied;
	}

	public void setApplied(boolean isApplied) {
		this.isApplied = isApplied;
	}

	public static Comparator<ParallelSnapshotIsolationPiggyback> getParallelSnapshotIsolationPiggybackComparator() {
		return ParallelSnapshotIsolationPiggybackComparator;
	}

	public TransactionHandler getTransactionHandler() {
		return transactionHandler;
	}

	
	
}
