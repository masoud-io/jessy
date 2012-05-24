package fr.inria.jessy;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.MessageStream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.PerformanceProbe;
import net.sourceforge.fractal.utils.PerformanceProbe.SimpleCounter;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.yahoo.ycsb.YCSBEntity;

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
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.termination.message.VoteMessage;
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

	public FractalManager fractal;
	public Membership membership;
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

			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					ConstantPool.CONFIG_PROPERTY);
			myProps.load(MyInputStream);
			String fractalFile = myProps.getProperty(ConstantPool.FRACTAL_FILE);
			MyInputStream.close();

			// Initialize Fractal: create server groups, initialize this node
			// and create the global group.
			fractal = FractalManager.getInstance();
			fractal.loadFile(fractalFile);
			membership = fractal.membership;
			membership.dispatchPeers(ConstantPool.JESSY_SERVER_GROUP,
					ConstantPool.JESSY_SERVER_PORT, ConstantPool.GROUP_SIZE);
			membership.loadIdenitity(null);
			Group replicaGroup = !membership.myGroups().isEmpty() ? membership
					.myGroups().iterator().next() : null; // this node is a
															// server ?
			Group allGroup = fractal.membership.getOrCreateTCPDynamicGroup(
					ConstantPool.JESSY_ALL_GROUP, ConstantPool.JESSY_ALL_PORT);
			allGroup.putNodes(fractal.membership.allNodes());
			fractal.start();

			if (replicaGroup != null) {
				logger.info("Server mode (" + replicaGroup + ")");
				distributedTermination = new DistributedTermination(this,
						replicaGroup);
			} else {
				logger.info("Proxy mode");
				distributedTermination = new DistributedTermination(this,
						allGroup);
			}

			remoteReader = new RemoteReader(this, allGroup);
			partitioner = new Partitioner(membership);

			// FIXME
			MessageStream.addClass(YCSBEntity.class.getName());
			super.addEntity(YCSBEntity.class);
			partitioner.assign(YCSBEntity.keyspace);
			// TODO for TPCC classes.

			
			// FIXME MOVE THIS
			MessageStream.addClass(JessyEntity.class.getName());
			MessageStream.addClass(EntitySet.class.getName());
			MessageStream.addClass(Vector.class.getName());
			MessageStream.addClass(ValueVector.class.getName());
			MessageStream.addClass(DependenceVector.class.getName());
			MessageStream.addClass(NullVector.class.getName());
			MessageStream.addClass(ReadReply.class.getName());
			MessageStream.addClass(ReadRequest.class.getName());
			MessageStream.addClass(RemoteReadRequestMessage.class.getName());
			MessageStream.addClass(RemoteReadReplyMessage.class.getName());
			MessageStream.addClass(VoteMessage.class.getName());
			MessageStream.addClass(Vote.class.getName());
			MessageStream.addClass(TerminateTransactionRequestMessage.class.getName());
			MessageStream.addClass(TerminateTransactionRequestMessage.class.getName());
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
		ReadReply<E> readReply;
		if (partitioner.isLocal(readRequest.getPartitioningKey())) {
			logger.debug("performing local read on " + keyValue
					+ " for request " + readRequest);
			readReply = getDataStore().get(readRequest);
		} else {
			logger.debug("performing remote read on " + keyValue
					+ " for request " + readRequest);
			remoteReads.incr();
			Future<ReadReply<E>> future = remoteReader.remoteRead(readRequest);
			readReply = future.get();
		}
		readRequestTime.add(System.nanoTime()-start);

		if ( readReply != null
			 &&	readReply.getEntity() != null 
			 && readReply.getEntity().iterator().hasNext()
			 && readReply.getEntity().iterator().next() != null) { // FIXME improve this
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
		ReadReply<E> readReply;
		if (partitioner.isLocal(readRequest.getPartitioningKey())) {
			logger.debug("performing local read on " + keys + " for request "
					+ readRequest);
			readReply = getDataStore().get(readRequest);
		} else {
			logger.debug("performing remote read on " + keys + " for request "
					+ readRequest);
			remoteReads.incr();
			Future<ReadReply<E>> future = remoteReader.remoteRead(readRequest);
			readReply = future.get();
		}
		readRequestTime.add(System.nanoTime()-start);

		if (readReply.getEntity().iterator().hasNext()
				&& readReply.getEntity().iterator().next() != null) {
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

		// 1 - Create a blind write transaction.
		TransactionHandler transactionHandler = new TransactionHandler();
		ExecutionHistory executionHistory = new ExecutionHistory(
				transactionHandler);
		executionHistory.addWriteEntity(entity);

		// 2 - Submit it to the termination protocol.
		Future<TransactionState> result = distributedTermination
				.terminateTransaction(executionHistory);
		result.get();

		NonTransactionalWriteRequestTime.stop();
	}

	public <E extends JessyEntity> void performNonTransactionalLocalWrite(
			E entity) throws InterruptedException, ExecutionException {
		dataStore.put(entity);
	}

	// I think it should only be
	// syncrhonized during certification. 
	// Thus, it is safe before certification test.
	@Override
	public ExecutionHistory commitTransaction(
			TransactionHandler transactionHandler) {
		logger.debug(transactionHandler + " IS COMMITTING");
		ExecutionHistory executionHistory = getExecutionHistory(transactionHandler);
		Future<TransactionState> stateFuture= distributedTermination
				.terminateTransaction(executionHistory);

		TransactionState stateResult= null;
		try {
			stateResult = stateFuture.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		assert (stateResult != null);
		executionHistory.changeState(stateResult);

		logger.debug(transactionHandler + " "+ stateResult);
		return executionHistory;
	}

	@Override
	public void open() {
		logger.info("Jessy is opened.");
	}

	public void close(Object object) {
		super.close(this);
		logger.info("Jessy is closed.");
	}

	/**
	 * Main entry point to Distributed Jessy Runtime.
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		try {

			// Logging.
			PerformanceProbe.setOutput("/dev/stdout");
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
