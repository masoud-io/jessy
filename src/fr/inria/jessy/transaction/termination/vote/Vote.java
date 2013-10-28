package fr.inria.jessy.transaction.termination.vote;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.protocol.ProtocolFactory;
import fr.inria.jessy.transaction.TransactionHandler;

public class Vote implements Externalizable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	private TransactionHandler transactionHandler;
	private boolean isCommitted;
	private String voterEntityName;

	private VotePiggyback votePiggyback;

	@Deprecated
	public Vote() {
	}

	public Vote(TransactionHandler transactionHandler, boolean committed,
			String votertEntityName, VotePiggyback votePiggyback) {
		this.transactionHandler = transactionHandler;
		this.isCommitted = committed;
		this.voterEntityName = votertEntityName;
		this.votePiggyback = votePiggyback;
	}

	public TransactionHandler getTransactionHandler() {
		return transactionHandler;
	}

	public boolean isCommitted() {
		return isCommitted;
	}

	public String getVoterEntityName() {
		return voterEntityName;
	}

	public VotePiggyback getVotePiggyBack() {
		return votePiggyback;
	}
	
	public void setVoterEntityName(String voterEntityName){
		this.voterEntityName=voterEntityName;
	}
	
	public String toString(){
		return "VOTE["+this.transactionHandler.toString()+"]";
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		transactionHandler = (TransactionHandler) in.readObject();
		isCommitted = in.readBoolean();
		voterEntityName = (String) in.readObject();

		if (ProtocolFactory.getProtocolInstance()
				.isVotePiggybackRequired())
			votePiggyback = (VotePiggyback) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(transactionHandler);
		out.writeBoolean(isCommitted);
		out.writeObject(voterEntityName);

		if (ProtocolFactory.getProtocolInstance()
				.isVotePiggybackRequired())
			out.writeObject(votePiggyback);
	}

}
