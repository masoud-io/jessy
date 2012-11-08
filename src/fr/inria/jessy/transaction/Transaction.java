package fr.inria.jessy.transaction;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

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
	private static Logger logger = Logger.getLogger(Transaction.class);

	private static ValueRecorder transactionExecutionTime_ReadOlny;
	private static ValueRecorder transactionExecutionTime_Update;
	private static ValueRecorder transactionTerminationTime_ReadOnly;
	private static ValueRecorder transactionTerminationTime_Update;
	private static ValueRecorder transactionReadOperatinTime;

	static {
		// Performance measuring facilities

		transactionExecutionTime_ReadOlny = new ValueRecorder(
				"Transaction#transactionExecutionTime_ReadOlny(ms)");
		transactionExecutionTime_ReadOlny.setFormat("%a");
		transactionExecutionTime_ReadOlny.setFactor(1000000);

		transactionExecutionTime_Update = new ValueRecorder(
				"Transaction#transactionExecutionTime_Update(ms)");
		transactionExecutionTime_Update.setFormat("%a");
		transactionExecutionTime_Update.setFactor(1000000);

		transactionTerminationTime_ReadOnly = new ValueRecorder(
				"Transaction#transactionTerminationTime_ReadOnly(ms)");
		transactionTerminationTime_ReadOnly.setFormat("%a");
		transactionTerminationTime_ReadOnly.setFactor(1000000);

		transactionTerminationTime_Update = new ValueRecorder(
				"Transaction#transactionTerminationTime_Update(ms)");
		transactionTerminationTime_Update.setFormat("%a");
		transactionTerminationTime_Update.setFactor(1000000);

		transactionReadOperatinTime = new ValueRecorder(
				"Transaction#transactionReadOperatinTime(ms)");
		transactionReadOperatinTime.setFormat("%a");
		transactionReadOperatinTime.setFactor(1000000);

		retryCommitOnAbort = readConfig();
	}

	long executionStartTime;

	private Jessy jessy;
	private TransactionHandler transactionHandler;
	private boolean isQuery;

	private static boolean retryCommitOnAbort;

	public Transaction(Jessy jessy) throws Exception {
		this.jessy = jessy;
		this.transactionHandler = jessy.startTransaction();
		this.isQuery = true;
		executionStartTime = System.nanoTime();
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
		long start = System.nanoTime();
		E entity = jessy.read(transactionHandler, entityClass, keyValue);
		transactionReadOperatinTime.add(System.nanoTime() - start);
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
		isQuery = false;
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

		if (isQuery)
			transactionExecutionTime_ReadOlny.add(System.nanoTime()
					- executionStartTime);
		else
			transactionExecutionTime_Update.add(System.nanoTime()
					- executionStartTime);

		long startterm = System.nanoTime();

		ExecutionHistory executionHistory = jessy
				.commitTransaction(transactionHandler);

		if (executionHistory.getTransactionState() != TransactionState.COMMITTED
				&& retryCommitOnAbort) {

			try {

				logger.warn("Re-executing aborted "
						+ (isQuery ? "(query)" : "") + " transaction "
						+ executionHistory.getTransactionHandler());

				/*
				 * Garbage collect the older execution. We do not need it
				 * anymore.
				 */
				jessy.garbageCollectTransaction(transactionHandler);

				/*
				 * must have a new handler.
				 */
				this.transactionHandler = jessy.startTransaction();

				executionHistory = execute();

			} catch (Exception e) {
				// FIXME abort properly
				e.printStackTrace();
			}

		}

		jessy.garbageCollectTransaction(transactionHandler);

		if (isQuery)
			transactionTerminationTime_ReadOnly.add(System.nanoTime()
					- startterm);
		else
			transactionTerminationTime_Update.add(System.nanoTime()
					- startterm);

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

	public static void setRetryCommitOnAbort(boolean retryCommitOnAbort) {
		Transaction.retryCommitOnAbort = retryCommitOnAbort;
	}

	public static boolean getRetryCommitOnAbort() {
		return Transaction.retryCommitOnAbort;
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
