package fr.inria.jessy.transaction.termination;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.TransactionHandler;

public class Vote implements Externalizable {
	
	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	
	private TransactionHandler transactionHandler;
	private boolean isAborted;
	private String voterGroupName;

	@Deprecated
	public Vote(){
	}
	
	public Vote(TransactionHandler transactionHandler, boolean aborted, String voterGroupName) {
		this.transactionHandler = transactionHandler;
		this.isAborted = aborted;
		this.voterGroupName = voterGroupName;
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

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		transactionHandler = (TransactionHandler) in.readObject();
		isAborted = in.readBoolean();
		voterGroupName = (String) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(transactionHandler);
		out.writeBoolean(isAborted);
		out.writeObject(voterGroupName);
		
	}

}
