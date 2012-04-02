package fr.inria.jessy.transaction.termination;

import java.io.Serializable;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;

/**
 * This class is used for sending back the result of a transaction termination
 * to its coordinator (proxy).
 * <p>
 * It is used instead of {@link ExecutionHistory} since {@link ExecutionHistory}
 * contains unneeded information, and its size can possibly be very large.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class TerminationResult implements Serializable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	private TransactionHandler transactionHandler;
	private TransactionState transactionState;
	private Boolean sendBackToCoordinator;

	TerminationResult(TransactionHandler transactionHandler,
			TransactionState transactionState, Boolean sendBackToCoordinator) {
		this.transactionHandler = transactionHandler;
		this.transactionState = transactionState;
		this.sendBackToCoordinator = sendBackToCoordinator;
	}

	public TransactionHandler getTransactionHandler() {
		return transactionHandler;
	}

	public TransactionState getTransactionState() {
		return transactionState;
	}

	public Boolean isSendBackToCoordinator() {
		return sendBackToCoordinator;
	}

}
