package fr.inria.jessy.transaction;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import net.sourceforge.fractal.utils.PerformanceProbe.SimpleCounter;
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

	public static SimpleCounter totalCount = new SimpleCounter(
			"Transaction#TotalTransactions");
	
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

		transactionExecutionTime_Update = new ValueRecorder(
				"Transaction#transactionExecutionTime_Update(ms)");
		transactionExecutionTime_Update.setFormat("%a");

		transactionTerminationTime_ReadOnly = new ValueRecorder(
				"Transaction#transactionTerminationTime_ReadOnly(ms)");
		transactionTerminationTime_ReadOnly.setFormat("%a");

		transactionTerminationTime_Update = new ValueRecorder(
				"Transaction#transactionTerminationTime_Update(ms)");
		transactionTerminationTime_Update.setFormat("%a");

		transactionReadOperatinTime = new ValueRecorder(
				"Transaction#transactionReadOperatinTime(ms)");
		transactionReadOperatinTime.setFormat("%a");

		retryCommitOnAbort = readConfig();
	}

	/**
	 * Start time of the execution phase in nanoseconds.
	 * <p>
	 * If the transaction aborts, the resulting transaction will again calculate the execution phase.
	 * This is for the sake of simplicity and should not change the result. 
	 */
	private long executionStartTime;
	
	/**
	 * Start time of the termination phase in nanoseconds.
	 * <p>
	 * If the transaction aborts, the resulting transaction will NOT calculate the termination phase of the resulting transaction.
	 * The termination latency is only calculated for the main transaction from the starting of its termination until it commits when 
	 * {@code Transaction#mainTransactionCommit} is zero. 
	 */ 
	private long terminationStartTime;
	
	/**
	 * If zero, it means the commit is for the main transaction.
	 * If greater than zero, it means the main transaction aborts, and this commit is for the resulting transaction.
	 * This variable is crucial to compute the correct termination latency. In other words, termination latency is only computed for
	 * the main transaction inside the commit method. 
	 */
	private int mainTransactionCommit=0;

	private Jessy jessy;
	private TransactionHandler transactionHandler;
	private boolean isQuery;

	private static boolean retryCommitOnAbort;

	public Transaction(Jessy jessy) throws Exception {
		this.jessy = jessy;
		this.transactionHandler = jessy.startTransaction();
		this.isQuery = true;
		executionStartTime = System.currentTimeMillis();
		totalCount.incr();
	}
	
	public Transaction(Jessy jessy, int readOperations, int updateOperations, int createOperations) throws Exception {
		this.jessy = jessy;
		this.transactionHandler = jessy.startTransaction(readOperations,updateOperations,createOperations);
		this.isQuery = true;
		executionStartTime = System.currentTimeMillis();
		totalCount.incr();
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
		long start = System.currentTimeMillis();
		E entity = jessy.read(transactionHandler, entityClass, keyValue);
		transactionReadOperatinTime.add(System.currentTimeMillis() - start);
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
	 */
	public ExecutionHistory commitTransaction() {
		if(mainTransactionCommit==0) 
			terminationStartTime= System.currentTimeMillis();
		
		mainTransactionCommit++;
		
		if (isQuery)
			transactionExecutionTime_ReadOlny.add(System.currentTimeMillis()
					- executionStartTime);
		else{
			transactionExecutionTime_Update.add(System.currentTimeMillis()
					- executionStartTime);
		}

		ExecutionHistory executionHistory = jessy
				.commitTransaction(transactionHandler);

		if (executionHistory.getTransactionState() != TransactionState.COMMITTED
				&& retryCommitOnAbort) {

			try {

				if (ConstantPool.logging)
					logger.warn("Re-executing aborted "
							+ (isQuery ? "(query)" : "") + " transaction "
							+ executionHistory.getTransactionHandler() + " . Reason : " + executionHistory.getTransactionState() );

				/*
				 * Garbage collect the older execution. We do not need it
				 * anymore.
				 */
				jessy.garbageCollectTransaction(transactionHandler);

				/*
				 * must have a new handler.
				 */
				TransactionHandler oldHanlder=this.transactionHandler.clone();
				this.transactionHandler = jessy.startTransaction();
				if (executionHistory.getTransactionState()==TransactionState.ABORTED_BY_TIMEOUT)
					this.transactionHandler.setPreviousTimedoutTransactionHandler(oldHanlder);
				reInitProbes();
				mainTransactionCommit++;
				executionHistory = execute();
				mainTransactionCommit--;

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		jessy.garbageCollectTransaction(transactionHandler);

		mainTransactionCommit--;
		if (mainTransactionCommit==0)
			if (isQuery)
				transactionTerminationTime_ReadOnly.add(System.currentTimeMillis()
						- terminationStartTime);
			else
				transactionTerminationTime_Update.add(System.currentTimeMillis()
						- terminationStartTime);

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

	/**
	 * Since the transaction can abort, and re-executing the aborted transaction is performed inside the {@code Transaction#commitTransaction()}
	 * we need to re set all the probes. Otherwise, the execution latency is not accurate since the start time is the very beginning of the transaction.
	 */
	private void reInitProbes(){
		executionStartTime = System.currentTimeMillis();
	}
}
