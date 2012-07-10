package fr.inria.jessy;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import net.sourceforge.fractal.MessageStream;
import net.sourceforge.fractal.utils.PerformanceProbe;
import net.sourceforge.fractal.utils.PerformanceProbe.FloatValueRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.SimpleCounter;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.yahoo.ycsb.YCSBEntity;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.ReadReplyMessage;
import fr.inria.jessy.communication.message.ReadRequestMessage;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.partitioner.Partitioner;
import fr.inria.jessy.store.EntitySet;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.DistributedTermination;
import fr.inria.jessy.transaction.termination.Vote;
import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.DependenceVector;
import fr.inria.jessy.vector.NullVector;
import fr.inria.jessy.vector.ValueVector;
import fr.inria.jessy.vector.Vector;

public class DistributedJessy extends Jessy {

	private static Logger logger = Logger.getLogger(DistributedJessy.class);
	private static DistributedJessy distributedJessy = null;

	private static SimpleCounter remoteReads;
	private static TimeRecorder NonTransactionalWriteRequestTime;
	private static ValueRecorder readRequestTime;

	private static SimpleCounter executionCount = new SimpleCounter(
			"Jessy#TotalExecution");
	private static SimpleCounter abortByVoteCount = new SimpleCounter(
			"Jessy#abortByVoteCount");
	private static SimpleCounter abortByCertificationCount = new SimpleCounter(
			"Jessy#abortByCertificationCount");
	private static SimpleCounter abortByTimeout = new SimpleCounter(
			"Jessy#abortByTimeout");

	private static FloatValueRecorder totalRatioAbortedTransactions = new FloatValueRecorder(
			"Jessy#ratioAbortedTransactions");
	private static FloatValueRecorder voteRatioAbortedTransactions = new FloatValueRecorder(
			"Jessy#voteRatioAbortedTransactions");
	private static FloatValueRecorder certificationRatioAbortedTransactions = new FloatValueRecorder(
			"Jessy#certificationRatioAbortedTransactions");
	private static FloatValueRecorder timeoutRatioAbortedTransactions = new FloatValueRecorder(
			"Jessy#timeoutRatioAbortedTransactions");

	private static FloatValueRecorder ratioFailedReads = new FloatValueRecorder(
			"Jessy#ratioFailedReads");

	public RemoteReader remoteReader;
	public DistributedTermination distributedTermination;
	public Partitioner partitioner;

	static {
		// Performance measuring facilities
		remoteReads = new SimpleCounter("Jessy#RemoteReads");
		NonTransactionalWriteRequestTime = new TimeRecorder(
				"Jessy#NonTransactionalWriteRequestTime");
		readRequestTime = new ValueRecorder("Jessy#ReadRequestTime(us)");
		readRequestTime.setFormat("%a");
		readRequestTime.setFactor(1000);

	}

	private DistributedJessy() throws Exception {
		super();
		try {

			PerformanceProbe.setOutput("/dev/stdout");

			distributedTermination = new DistributedTermination(this);

			remoteReader = new RemoteReader(this);

			// FIXME
			MessageStream.addClass(YCSBEntity.class.getName());
			super.addEntity(YCSBEntity.class);

			partitioner = JessyGroupManager.getInstance().getPartitioner();

			// FIXME MOVE THIS
			MessageStream.addClass(JessyEntity.class.getName());
			MessageStream.addClass(EntitySet.class.getName());
			MessageStream.addClass(Vector.class.getName());
			MessageStream.addClass(ValueVector.class.getName());
			MessageStream.addClass(DependenceVector.class.getName());
			MessageStream.addClass(NullVector.class.getName());
			MessageStream.addClass(ReadReply.class.getName());
			MessageStream.addClass(ReadRequest.class.getName());
			MessageStream.addClass(ReadRequestMessage.class.getName());
			MessageStream.addClass(ReadReplyMessage.class.getName());
			MessageStream.addClass(VoteMessage.class.getName());
			MessageStream.addClass(Vote.class.getName());
			MessageStream.addClass(TerminateTransactionRequestMessage.class
					.getName());
			MessageStream.addClass(ExecutionHistory.class.getName());
			MessageStream.addClass(TransactionHandler.class.getName());

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static synchronized DistributedJessy getInstance() {
		if (distributedJessy == null) {
			try {
				distributedJessy = new DistributedJessy();
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage());
			}
		}
		return distributedJessy;
	}

	@Override
	protected <E extends JessyEntity, SK> E performRead(Class<E> entityClass,
			String keyName, SK keyValue, CompactVector<String> readSet)
			throws InterruptedException, ExecutionException {

		long start = System.nanoTime();
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keyName,
				keyValue, readSet);
		ReadReply<E> readReply = null;
		if (partitioner.isLocal(readRequest.getPartitioningKey())) {
			logger.debug("performing local read on " + keyValue
					+ " for request " + readRequest);
			readReply = getDataStore().get(readRequest);
		} else {
			logger.debug("performing remote read on " + keyValue
					+ " for request " + readRequest);
			remoteReads.incr();

			Future<ReadReply<E>> future;
			boolean isDone = false;
			do {
				future = remoteReader.remoteRead(readRequest);
				try {
					readReply = future.get(
							ConstantPool.JESSY_REMOTE_READER_TIMEOUT,
							ConstantPool.JESSY_REMOTE_READER_TIMEOUT_TYPE);
					isDone = true;
				} catch (TimeoutException e) {
					/*
					 * Nothing to do. The message should have been lost. Retry
					 * again.
					 */
				}
			} while (!isDone);

		}
		readRequestTime.add(System.nanoTime() - start);

		if (readReply != null && readReply.getEntity() != null
				&& readReply.getEntity().iterator().hasNext()
				&& readReply.getEntity().iterator().next() != null) { // FIXME
																		// improve
																		// this
			logger.debug("read " + readRequest + " is successfull ");
			return readReply.getEntity().iterator().next();
		} else {
			logger.debug("request " + readRequest + " failed");
			return null;
		}

	}

	@Override
	protected <E extends JessyEntity> Collection<E> performRead(
			Class<E> entityClass, List<ReadRequestKey<?>> keys,
			CompactVector<String> readSet) throws InterruptedException,
			ExecutionException {

		long start = System.nanoTime();
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keys,
				readSet);
		ReadReply<E> readReply = null;
		if (partitioner.isLocal(readRequest.getPartitioningKey())) {
			logger.debug("performing local read on " + keys + " for request "
					+ readRequest);
			readReply = getDataStore().get(readRequest);
		} else {
			logger.debug("performing remote read on " + keys + " for request "
					+ readRequest);
			remoteReads.incr();

			Future<ReadReply<E>> future;
			boolean isDone = false;
			do {
				future = remoteReader.remoteRead(readRequest);
				try {
					readReply = future.get(
							ConstantPool.JESSY_REMOTE_READER_TIMEOUT,
							ConstantPool.JESSY_REMOTE_READER_TIMEOUT_TYPE);
					isDone = true;
				} catch (TimeoutException e) {
					/*
					 * Nothing to do. The message should have been lost. Retry
					 * again.
					 */
				}
			} while (!isDone);

		}
		readRequestTime.add(System.nanoTime() - start);

		if (readReply != null && readReply.getEntity() != null
				&& readReply.getEntity().iterator().hasNext()
				&& readReply.getEntity().iterator().next() != null) { // FIXME
																		// improve
																		// this
			logger.debug("read " + readRequest + " is successfull ");
			return readReply.getEntity();
		} else {
			logger.debug("request " + readRequest + " failed");
			return null;
		}
	}

	@Override
	public <E extends JessyEntity> void performNonTransactionalWrite(E entity)
			throws InterruptedException, ExecutionException {

		NonTransactionalWriteRequestTime.start();

		logger.debug("performing Write to " + entity.getKey());

		// if (partitioner.isLocal(entity.getKey()) && ConstantPool.GROUP_SIZE
		// == 1) {
		// performNonTransactionalLocalWrite(entity);
		// } else {

		// 1 - Create a blind write transaction.
		TransactionHandler transactionHandler = new TransactionHandler();
		ExecutionHistory executionHistory = new ExecutionHistory(
				transactionHandler);
		executionHistory.addWriteEntity(entity);

		// 2 - Submit it to the termination protocol.
		Future<TransactionState> result = distributedTermination
				.terminateTransaction(executionHistory);
		result.get();

		garbageCollectTransaction(transactionHandler);

		NonTransactionalWriteRequestTime.stop();
	}

	public <E extends JessyEntity> void performNonTransactionalLocalWrite(
			E entity) throws InterruptedException, ExecutionException {
		dataStore.put(entity);
	}

	@Override
	public void applyModifiedEntities(ExecutionHistory executionHistory) {
		Iterator<? extends JessyEntity> itr;

		if (executionHistory.getWriteSet().size() > 0) {
			itr = executionHistory.getWriteSet().getEntities().iterator();
			while (itr.hasNext()) {
				JessyEntity tmp = itr.next();

				// Send the entity to the datastore to be saved if it is local
				if (partitioner.isLocal(tmp.getKey()))
					dataStore.put(tmp);
			}
		}

		if (executionHistory.getCreateSet().size() > 0) {
			itr = executionHistory.getCreateSet().getEntities().iterator();
			while (itr.hasNext()) {
				JessyEntity tmp = itr.next();

				// Send the entity to the datastore to be saved if it is local
				if (partitioner.isLocal(tmp.getKey()))
					dataStore.put(tmp);
			}
		}
	}

	@Override
	public ExecutionHistory commitTransaction(
			TransactionHandler transactionHandler) {
		logger.debug(transactionHandler + " IS COMMITTING");
		ExecutionHistory executionHistory = getExecutionHistory(transactionHandler);
		Future<TransactionState> stateFuture = distributedTermination
				.terminateTransaction(executionHistory);

		TransactionState stateResult = null;
		try {
			stateResult = stateFuture.get(
					ConstantPool.JESSY_TRANSACTION_TERMINATION_TIMEOUT,
					ConstantPool.JESSY_TRANSACTION_TERMINATION_TIMEOUT_TYPE);
			executionHistory.changeState(stateResult);
		} catch (TimeoutException te) {
			executionHistory.changeState(TransactionState.ABORTED_BY_TIMEOUT);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assert (stateResult != null);

		/*
		 * Set the probes for calculating the abort rate.
		 */
		executionCount.incr();
		if (stateResult == TransactionState.ABORTED_BY_VOTING)
			abortByVoteCount.incr();
		else if (stateResult == TransactionState.ABORTED_BY_CERTIFICATION)
			abortByCertificationCount.incr();
		else if (stateResult == TransactionState.ABORTED_BY_TIMEOUT)
			abortByTimeout.incr();

		logger.debug(transactionHandler + " " + stateResult);
		return executionHistory;
	}

	@Override
	public void open() {
		super.open();
		logger.info("Jessy is opened.");
	}

	public void close(Object object) {

		activeClients.remove(object);
		if (activeClients.size() == 0) {

			totalRatioAbortedTransactions.setFormat("%t");
			totalRatioAbortedTransactions
					.add((Double.valueOf(abortByVoteCount.toString())
							+ Double.valueOf(abortByCertificationCount
									.toString()) + Double
							.valueOf(abortByTimeout.toString()))
							/ (Double.valueOf(executionCount.toString())));

			voteRatioAbortedTransactions.setFormat("%t");
			voteRatioAbortedTransactions.add(Double.valueOf(abortByVoteCount
					.toString()) / (Double.valueOf(executionCount.toString())));

			certificationRatioAbortedTransactions.setFormat("%t");
			certificationRatioAbortedTransactions.add(Double
					.valueOf(abortByCertificationCount.toString())
					/ (Double.valueOf(executionCount.toString())));

			timeoutRatioAbortedTransactions.setFormat("%t");
			timeoutRatioAbortedTransactions.add(Double.valueOf(abortByTimeout
					.toString()) / (Double.valueOf(executionCount.toString())));

			ratioFailedReads.setFormat("%t");
			ratioFailedReads.add(Double.valueOf(failedReadCount.toString())
					/ (Double.valueOf(executionCount.toString())));

			if (!JessyGroupManager.getInstance().isProxy()) {
				super.close(this);
				logger.info("Jessy is closed.");
			}
		}
	}

	/**
	 * Main entry point to Distributed Jessy Runtime.
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		try {
			PropertyConfigurator.configure("log4j.properties");
			final DistributedJessy j = DistributedJessy.getInstance();
			j.open();
			SignalHandler sh = new SignalHandler() {
				@Override
				public void handle(Signal s) {
					j.close(null);
					System.exit(0);
				}
			};
			Signal.handle(new Signal("INT"), sh);
			Signal.handle(new Signal("TERM"), sh);
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
