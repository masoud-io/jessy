package fr.inria.jessy.transaction;

public enum TransactionState {
	/**
	 * the transaction has not strated yet.
	 */
	NOT_STARTED,
	/**
	 * the transaction has been started and is executing
	 */
	EXECUTING,
	/**
	 * the transaction has been executed and is committing
	 */
	COMMITTING,
	/**
	 * the transaction has been committed
	 */
	COMMITTED,
	/**
	 * the transaction has been aborted because of the certification test
	 */
	ABORTED_BY_CERTIFICATION,
	/**
	 * the transaction has been aborted by the client.
	 */
	ABORTED_BY_CLIENT,
}
