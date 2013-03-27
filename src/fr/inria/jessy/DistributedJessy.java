package fr.inria.jessy;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

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

import fr.inria.jessy.ConstantPool.CAST_MODE;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.partitioner.Partitioner;
import fr.inria.jessy.persistence.FilePersistence;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.DistributedTermination;
import fr.inria.jessy.vector.CompactVector;

public class DistributedJessy extends Jessy {

	private static Logger logger = Logger.getLogger(DistributedJessy.class);
	private static DistributedJessy distributedJessy = null;

	private static TimeRecorder NonTransactionalWriteRequestTime;

	private static SimpleCounter executionCount = new SimpleCounter(
			"Jessy#TotalExecution");
	private static SimpleCounter abortByVoteCount = new SimpleCounter(
			"Jessy#abortByVoteCount");
	private static SimpleCounter abortByCertificationCount = new SimpleCounter(
			"Jessy#abortByCertificationCount");
	private static SimpleCounter abortByTimeout = new SimpleCounter(
			"Jessy#abortByTimeout");

	private static FloatValueRecorder ratioFailedTermination = new FloatValueRecorder(
			"Jessy#ratioFailedTermination");
	private static FloatValueRecorder voteRatioAbortedTransactions = new FloatValueRecorder(
			"Jessy#voteRatioAbortedTransactions");
	private static FloatValueRecorder certificationRatioAbortedTransactions = new FloatValueRecorder(
			"Jessy#certificationRatioAbortedTransactions");
	private static FloatValueRecorder timeoutRatioAbortedTransactions = new FloatValueRecorder(
			"Jessy#timeoutRatioAbortedTransactions");

	private static FloatValueRecorder ratioFailedReads = new FloatValueRecorder(
			"Jessy#ratioFailedReads");
	private static FloatValueRecorder ratioFailedExecution = new FloatValueRecorder(
			"Jessy#ratioFailedExecution");

	private static ValueRecorder remoteReaderLatency, clientProcessingResponseTime;
	
	public RemoteReader remoteReader;
	public DistributedTermination distributedTermination;
	public Partitioner partitioner;

	static {
		// Performance measuring facilities
		NonTransactionalWriteRequestTime = new TimeRecorder(
				"Jessy#NonTransactionalWriteRequestTime");
		
		remoteReaderLatency = new ValueRecorder(
				"DistributedJessy#remoteReaderLatency(ms,max)");
		remoteReaderLatency.setFormat("%M");
		
		clientProcessingResponseTime = new TimeRecorder("RemoteReader#clientProcessingResponseTime(ms)");
		clientProcessingResponseTime.setFormat("%a");
	}

	public DistributedJessy() throws Exception {
		super();
		try {

			PerformanceProbe.setOutput("/dev/stdout");

			distributedTermination = new DistributedTermination(this);

			if (ConstantPool.JESSY_REMOTE_READ_UNICST_MODE==CAST_MODE.FRACTAL)				
				remoteReader = new FractalRemoteReader(this);
			else
				remoteReader = new NettyRemoteReader(this);

			// FIXME
			super.addEntity(YCSBEntity.class);

			partitioner = manager.getPartitioner();

			// FIXME MOVE THIS
//			MessageStream.addClass(JessyEntity.class.getName());
//			MessageStream.addClass(YCSBEntity.class.getName());
//			MessageStream.addClass(ReadRequestKey.class.getName());
//
//			MessageStream.addClass(Vector.class.getName());
//			MessageStream.addClass(ValueVector.class.getName());
//			MessageStream.addClass(DependenceVector.class.getName());
//			MessageStream.addClass(NullVector.class.getName());
//			MessageStream.addClass(CompactVector.class.getName());
//			MessageStream.addClass(LightScalarVector.class.getName());
//			MessageStream.addClass(VersionVector.class.getName());
//			MessageStream.addClass(DependenceVector.class.getName());
//			MessageStream.addClass(ConcurrentVersionVector.class.getName());
//			MessageStream.addClass(GMUVector.class.getName());
//
//			MessageStream.addClass(ReadReply.class.getName());
//			MessageStream.addClass(ReadRequest.class.getName());
//			MessageStream.addClass(ReadRequestMessage.class.getName());
//			MessageStream.addClass(ReadReplyMessage.class.getName());
//			MessageStream
//					.addClass(ParallelSnapshotIsolationPropagateMessage.class
//							.getName());
//			MessageStream.addClass(ParallelSnapshotIsolationPiggyback.class
//					.getName());
//
//			MessageStream.addClass(VoteMessage.class.getName());
//			MessageStream.addClass(Vote.class.getName());
//			MessageStream.addClass(VotePiggyback.class.getName());
//
//			MessageStream.addClass(TerminateTransactionRequestMessage.class
//					.getName());
//
//			MessageStream.addClass(ExecutionHistory.class.getName());
//			MessageStream.addClass(TransactionHandler.class.getName());
//			MessageStream.addClass(EntitySet.class.getName());
//
//			MessageStream.addClass(Keyspace.class.getName());
//			MessageStream.addClass(TransactionTouchedKeys.class.getName());

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public DistributedJessy(JessyGroupManager m) throws Exception {
		
		super(m);
		
		try {

			PerformanceProbe.setOutput("/dev/stdout");

			distributedTermination = new DistributedTermination(this);

			if (ConstantPool.JESSY_REMOTE_READ_UNICST_MODE==CAST_MODE.FRACTAL)				
				remoteReader = new FractalRemoteReader(this);
			else
				remoteReader = new NettyRemoteReader(this);

			// FIXME
			super.addEntity(YCSBEntity.class);

			partitioner = manager.getPartitioner();

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

		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keyName,
				keyValue, readSet);
		ReadReply<E> readReply = null;
		E result = null;

		if (partitioner.isLocal(readRequest.getPartitioningKey())) {

			logger.debug("performing local read on " + keyValue
					+ " for request " + readRequest);
			readReply = getDataStore().get(readRequest);
			result = readReply.getEntity().iterator().next();
		} else {

			logger.debug("performing remote read on " + keyValue
					+ " for request " + readRequest.getOneKey().getKeyValue());

			Future<ReadReply<E>> future;
			boolean isDone = false;
			int tries = 0;

			long start=System.currentTimeMillis();
			do {

				future = remoteReader.remoteRead(readRequest);

				try {

					tries++;
					readReply = future.get(
							ConstantPool.JESSY_REMOTE_READER_TIMEOUT,
							ConstantPool.JESSY_REMOTE_READER_TIMEOUT_TYPE);

					if (readReply != null && readReply.getEntity() != null
							&& readReply.getEntity().iterator().hasNext()) {

						logger.debug("read " + readRequest + " is successfull ");
						result = readReply.getEntity().iterator().next();
						remoteReaderLatency.add(System.currentTimeMillis()-start);
						isDone = true;
					} 

				} catch (TimeoutException te) {					
					/*
					 * Nothing to do. The message should have been lost. Retry
					 * again.
					 */
					logger.error("TimeoutException happened in Distributed Jessy (Perform Read)" + te.getCause());
					failedReadCount.incr();
				} catch (InterruptedException ie) {
					logger.error("InterruptedException happened in Distributed Jessy (Perform Read)" + ie.getCause());
					failedReadCount.incr();
					isDone = true;
				} catch (ExecutionException ee) {
					logger.error("ExecutionException happened in Distributed Jessy (Perform Read)" + ee.getCause());
					failedReadCount.incr();
					isDone = true;
				}

			} while (!isDone);

		}

		return result;

	}

	@Override
	protected <E extends JessyEntity> Collection<E> performRead(
			Class<E> entityClass, List<ReadRequestKey<?>> keys,
			CompactVector<String> readSet) throws InterruptedException,
			ExecutionException {

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
				} catch (InterruptedException ie) {
					logger.error("InterruptedException happened in the remote reader");
				} catch (ExecutionException ee) {
					logger.error("ExecutionException happened in the remote reader");
				}
			} while (!isDone);

		}

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

				// Send the entity to the data store to be saved if it is local
				if (partitioner.isLocal(tmp.getKey()))
					dataStore.put(tmp);
			}
		}

		if (executionHistory.getCreateSet().size() > 0) {
			itr = executionHistory.getCreateSet().getEntities().iterator();
			while (itr.hasNext()) {
				JessyEntity tmp = itr.next();

				// Send the entity to the data store to be saved if it is local
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
		logger.info("Closing Jesss ...");

		activeClients.remove(object);
		if (activeClients.size() == 0) {
			ratioFailedTermination.setFormat("%t");
			ratioFailedTermination
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
					/ (Double.valueOf(totalReadCount.toString())));

			ratioFailedExecution.setFormat("%t");
			ratioFailedExecution.add(Double.valueOf(failedReadCount.toString())
					/ (Double.valueOf(executionCount.toString())));

			if (!manager.isProxy()) {
				super.close(this);
				FilePersistence.saveJessy();
				remoteReader.closeReplicaConnections();
				logger.info("Jessy is closed.");
			}
			else{
				remoteReader.closeProxyConnections();
				distributedTermination.closeConnections();
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
			
			JessyGroupManager m = new JessyGroupManager();
			
			for (String str :args){
				if (str.toLowerCase().contains("savetodisk")){
					System.out.println("Will save the state into disk at exit.");
					FilePersistence.saveToDisk=true;
				}
				else if (str.toLowerCase().contains("loadfromdisk")){
					FilePersistence.loadFromDisk=true;
					System.out.println("Will load the state from disk.");
				}
				else if (str.contains("/")){
					FilePersistence.makeStorageDirectory(m, str);
				}
				
			}
			
			final DistributedJessy j = new DistributedJessy(m);
			
			if ((FilePersistence.saveToDisk || FilePersistence.loadFromDisk) && FilePersistence.storageDirectory.equals("")){
				System.out.println("Please provide the path for loading/saving.");
			}
				
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

	@Override
	protected JessyGroupManager createJessyGroupManager() {
		return new JessyGroupManager();
	}

}
