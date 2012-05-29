package fr.inria.jessy.transaction;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequestKey;

/**
 * This class is the interface for transactional execution on top of Jessy.
 * 
 */
public abstract class Transaction implements Callable<ExecutionHistory> {

	private static TimeRecorder transactionTotalTime = new TimeRecorder(
			"Jessy#TransactionTotalTime");
	private static TimeRecorder transactionExecutionTime = new TimeRecorder(
			"Jessy#TransactionExecutionTime");
	private static TimeRecorder transactionTerminationTime = new TimeRecorder(
			"Jessy#TransactionTerminationTime"); 

	private static Logger logger = Logger.getLogger(Transaction.class);

	private Jessy jessy;
	private TransactionHandler transactionHandler;

	private static boolean retryCommitOnAbort = readConfig();

	public Transaction(Jessy jessy) throws Exception {
		this.jessy = jessy;

		transactionTotalTime.start();
		transactionExecutionTime.start();

		this.transactionHandler = jessy.startTransaction();

	}

	/**
	 * Execute the transaction logic.
	 */
	public abstract ExecutionHistory execute();

	/**
	 * Performs a transactional read on top of Jessy.
	 * 
	 * @param <E>
	 *            The type of the entity needed to be read.
	 * @param entityClass
	 *            The class of the entity needed to be read.
	 * @param keyValue
	 *            The key of the entity needed to be read.
	 * @return The read entity from jessy.
	 * @throws Exception
	 */
	public <E extends JessyEntity> E read(Class<E> entityClass, String keyValue)
			throws Exception {
		E entity = jessy.read(transactionHandler, entityClass, keyValue);
		// if (entity != null)
		// entity.setPrimaryKey(null);
		return entity;
	}

	public <E extends JessyEntity> Collection<E> read(Class<E> entityClass,
			List<ReadRequestKey<?>> keys) throws Exception {
		return jessy.read(transactionHandler, entityClass, keys);
	}

	public <E extends JessyEntity> void write(E entity)
			throws NullPointerException {
		entity.setPrimaryKey(null);

		jessy.write(transactionHandler, entity);
	}

	public <E extends JessyEntity> void create(E entity) {
		entity.setPrimaryKey(null);

		jessy.create(transactionHandler, entity);
		logger.info("entity " + entity.getKey() + " is created");
	}

	/*
	 * Tries to commit a transaction. If commit is not successful, and it is
	 * defined to retryCommitOnAbort, it will do so, until it commits the
	 * transaction.
	 * 
	 * FIXME Can it happen to abort a transaction indefinitely?
	 */
	public ExecutionHistory commitTransaction() {
		transactionExecutionTime.stop();
		transactionTerminationTime.start();

		ExecutionHistory executionHistory = jessy
				.commitTransaction(transactionHandler);

		if (executionHistory.getTransactionState() != TransactionState.COMMITTED
				&& retryCommitOnAbort) {
			try {
				/*
				 * Garbage collect the older execution. We do not need it
				 * anymore.
				 */
				jessy.garbageCollectTransaction(transactionHandler);

				/*
				 * must have a new handler.
				 */
				this.transactionHandler = jessy.startTransaction();

				logger.warn("Re-executing aborted transaction: "
						+ executionHistory.getTransactionHandler());
				executionHistory = execute();
			} catch (Exception e) {
				// FIXME abort properly
				e.printStackTrace();
			}

		}
		transactionTerminationTime.stop();
		jessy.garbageCollectTransaction(transactionHandler);
		transactionTotalTime.stop();
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

	private static boolean readConfig() {
		try {
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					ConstantPool.CONFIG_PROPERTY);
			myProps.load(MyInputStream);
			return myProps.getProperty(ConstantPool.RETRY_COMMIT)
					.equals("true") ? true : false;

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return true;
	}

}
