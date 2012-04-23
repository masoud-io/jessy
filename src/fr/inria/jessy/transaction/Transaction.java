package fr.inria.jessy.transaction;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequestKey;

//FIXME COMMENT ME PLZ!
public abstract class Transaction implements Callable<ExecutionHistory> {

	private static Logger logger = Logger.getLogger(Transaction.class);

	private Jessy jessy;
	private TransactionHandler transactionHandler;

	// TODO read from config file
	//TODO Test me
	private boolean retryCommitOnAbort = true;

	public Transaction(Jessy jessy) throws Exception {
		this.jessy = jessy;
		this.transactionHandler = jessy.startTransaction();
	}

	public abstract ExecutionHistory execute();

	public <E extends JessyEntity> E read(Class<E> entityClass, String keyValue)
			throws Exception {
		E entity = jessy.read(transactionHandler, entityClass, keyValue);
		if (entity != null)
			entity.setPrimaryKey(null);
		return entity;
	}

	public <E extends JessyEntity> Collection<E> read(Class<E> entityClass,
			List<ReadRequestKey<?>> keys) throws Exception {
		return jessy.read(transactionHandler, entityClass, keys);
	}

	public <E extends JessyEntity> void write(E entity)
			throws NullPointerException {
		jessy.write(transactionHandler, entity);
	}

	public <E extends JessyEntity> void create(E entity) {
		logger.info("Entity is created. >>" + entity.getKey());
		jessy.create(transactionHandler, entity);
	}

	/*
	 * Tries to commit a transaction. If commit is not successful, and it is
	 * defined to retryCommitOnAbort, it will do so, until it commits the
	 * transaction.
	 * 
	 * FIXME Can it happen to abort a transaction indefinitely?
	 * 
	 * TODO Re-execution was tested successfully for localJessy. The test for
	 * distributedJessy was not done extensively.
	 */
	public ExecutionHistory commitTransaction() {
		ExecutionHistory executionHistory = jessy
				.commitTransaction(transactionHandler);
		if (executionHistory.getTransactionState() != TransactionState.COMMITTED
				&& retryCommitOnAbort) {
			logger.warn("Re-executing aborted transaction: "
					+ executionHistory.getTransactionHandler());
			jessy.prepareReExecution(transactionHandler);
			executionHistory = execute();
		}
		jessy.garbageCollectTransaction(transactionHandler);
		return executionHistory;
	}

	public ExecutionHistory abortTransaction() {
		return jessy.abortTransaction(transactionHandler);
	}

	public ExecutionHistory call() {
		return execute();
	}

	public boolean isRetryCommitOnAbort() {
		return retryCommitOnAbort;
	}

	public void setRetryCommitOnAbort(boolean retryCommitOnAbort) {
		this.retryCommitOnAbort = retryCommitOnAbort;
	}
	
	

}
