package fr.inria.jessy.transaction.termination;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.transaction.TransactionHandler;

public class Vote implements Externalizable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	private TransactionHandler transactionHandler;
	private boolean isAborted;
	private String voterGroupName;
	public long startVoteTime;

	private VotePiggyback votePiggyback;

	@Deprecated
	public Vote() {
	}

	public Vote(TransactionHandler transactionHandler, boolean aborted,
			String voterGroupName, VotePiggyback votePiggyback) {
		this.transactionHandler = transactionHandler;
		this.isAborted = aborted;
		this.voterGroupName = voterGroupName;
		this.votePiggyback = votePiggyback;
	}

	public TransactionHandler getTransactionHandler() {
		return transactionHandler;
	}

	public boolean isAborted() {
		return isAborted;
	}

	public String getVoterGroupName() {
		return voterGroupName;
	}

	public VotePiggyback getVotePiggyBack() {
		return votePiggyback;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		startVoteTime=in.readLong();
		transactionHandler = (TransactionHandler) in.readObject();
		isAborted = in.readBoolean();
		voterGroupName = (String) in.readObject();

		if (ConsistencyFactory.getConsistencyInstance()
				.isVotePiggybackRequired())
			votePiggyback = (VotePiggyback) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(startVoteTime);
		out.writeObject(transactionHandler);
		out.writeBoolean(isAborted);
		out.writeObject(voterGroupName);

		if (ConsistencyFactory.getConsistencyInstance()
				.isVotePiggybackRequired())
			out.writeObject(votePiggyback);
	}

}
