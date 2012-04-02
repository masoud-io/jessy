package fr.inria.jessy.transaction;

import fr.inria.jessy.consistency.Consistency;

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
	 * the transaction has been aborted because of the certification test {@link Consistency#certify(java.util.concurrent.ConcurrentMap, ExecutionHistory)}
	 */
	ABORTED_BY_CERTIFICATION,
	/**
	 * the transaction has been aborted because of the abort vote from a remote site.
	 */
	ABORTED_BY_VOTING,
	/**
	 * the transaction has been aborted by the client.
	 */
	ABORTED_BY_CLIENT,
}
