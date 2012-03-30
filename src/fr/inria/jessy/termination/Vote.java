package fr.inria.jessy.termination;

import java.io.Serializable;
import java.util.Collection;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.TransactionHandler;

public class Vote implements Serializable {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	TransactionHandler transactionHandler;
	boolean certified;
	String voterGroupName;
	Collection<String> allVoterGroups;

	public Vote(TransactionHandler transactionHandler, boolean certified,
			String voterGroupName, Collection<String> allVoterGroups) {
		this.transactionHandler = transactionHandler;
		this.certified = certified;
		this.voterGroupName = voterGroupName;
		this.allVoterGroups = allVoterGroups;
	}

	public TransactionHandler getTransactionHandler() {
		return transactionHandler;
	}

	public boolean isCertified() {
		return certified;
	}

	public String getGroupName() {
		return voterGroupName;
	}

	public Collection<String> getAllVoterGroups() {
		return allVoterGroups;
	}
}
