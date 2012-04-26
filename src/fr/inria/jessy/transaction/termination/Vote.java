package fr.inria.jessy.transaction.termination;

import java.io.Serializable;
import java.util.Collection;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.TransactionHandler;

public class Vote implements Serializable {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	
	private TransactionHandler transactionHandler;
	private boolean isAborted;
	private String voterGroupName;
	private Collection<String> allVoterGroups;

	public Vote(TransactionHandler transactionHandler, boolean aborted,
			String voterGroupName, Collection<String> allVoterGroups) {
		this.transactionHandler = transactionHandler;
		this.isAborted = aborted;
		this.voterGroupName = voterGroupName;
		this.allVoterGroups = allVoterGroups;
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

	public Collection<String> getAllVoterGroups() {
		return allVoterGroups;
	}
}
