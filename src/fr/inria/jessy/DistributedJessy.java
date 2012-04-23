package fr.inria.jessy;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.PerformanceProbe;
import net.sourceforge.fractal.utils.PerformanceProbe.SimpleCounter;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import sun.misc.Signal;
import sun.misc.SignalHandler;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.DistributedTermination;
import fr.inria.jessy.transaction.termination.TerminationResult;
import fr.inria.jessy.vector.CompactVector;

public class DistributedJessy extends Jessy {

	private static Logger logger = Logger.getLogger(DistributedJessy.class);
	private static DistributedJessy distributedJessy = null;

	private static SimpleCounter remoteCall;
	private static TimeRecorder writeRequestTime, readRequestTime;

	public FractalManager fractal;
	public Membership membership;
	public RemoteReader remoteReader;
	public DistributedTermination distributedTermination;
	public Partitioner partitioner;

	static {
		// Performance measuring facilities
		remoteCall = new SimpleCounter("Jessy#RemoteCalls");
		writeRequestTime = new TimeRecorder("Jessy#WriteRequest");
		readRequestTime = new TimeRecorder("Jessy#ReadRequest");
	}

	private DistributedJessy() throws Exception {
		super();
		try {

			// FIXME move this.
			// Merge it in a PropertyHandler class (use Jean-Michel's work).
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					"config.property");
			myProps.load(MyInputStream);
			String fractalFile = myProps.getProperty("fractal_file");
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
				logger.info("Server mode");
				distributedTermination = new DistributedTermination(this,
						replicaGroup);
			} else {
				logger.info("Proxy mode");
				distributedTermination = new DistributedTermination(this,
						allGroup);
			}

			remoteReader = new RemoteReader(this, allGroup);
			partitioner = new Partitioner(membership);

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
	public <E extends JessyEntity> void addEntity(Class<E> entityClass)
	throws Exception {
		super.addEntity(entityClass);
		partitioner.assign(E.keyspace);
	}

	@Override
	protected <E extends JessyEntity, SK> E performRead(Class<E> entityClass,
			String keyName, SK keyValue, CompactVector<String> readSet)
			throws InterruptedException, ExecutionException {

		readRequestTime.start();
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keyName,
				keyValue, readSet);
		ReadReply<E> readReply;
		if (partitioner.isLocal(readRequest.getPartitioningKey())) {
			logger.debug("Performing Local Read for: " + keyValue);
			readReply = getDataStore().get(readRequest);
		} else {
			logger.debug("Performing Remote Read for: " + keyValue);
			remoteCall.incr();
			Future<ReadReply<E>> future = remoteReader.remoteRead(readRequest);
			readReply = future.get();
		}
		readRequestTime.stop();

		if (readReply.getEntity().iterator().hasNext())
			return readReply.getEntity().iterator().next();
		else
			return null;

	}

	@Override
	protected <E extends JessyEntity> Collection<E> performRead(
			Class<E> entityClass, List<ReadRequestKey<?>> keys,
			CompactVector<String> readSet) throws InterruptedException,
			ExecutionException {
		
		readRequestTime.start();
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keys,
				readSet);
		ReadReply<E> readReply;
		if (partitioner.isLocal(readRequest.getPartitioningKey())) {
			logger.debug("Performing Local Read for: " + keys);
			readReply = getDataStore().get(readRequest);
		} else {
			logger.debug("Performing Remote Read for: " + keys);
			remoteCall.incr();
			Future<ReadReply<E>> future = remoteReader.remoteRead(readRequest);
			readReply = future.get();
		}
		readRequestTime.stop();

		if (readReply.getEntity().iterator().hasNext())
			return readReply.getEntity();
		else
			return null;
	}

	@Override
	public <E extends JessyEntity> void performNonTransactionalWrite(E entity)
			throws InterruptedException, ExecutionException {

		writeRequestTime.start();
		// if (partitioner.isLocal(entity.getSecondaryKey())
		// && ConstantPool.REPLICATION_FACTOR == 1) {
		// logger.debug("Performing Local Write for: " +
		// entity.getSecondaryKey());
		// performNonTransactionalLocalWrite(entity);
		// } else {
		logger.debug("Performing Remote Write for: " + entity.getSecondaryKey());
		remoteCall.incr();
		// 1 - Create a blind write transaction.
		TransactionHandler transactionHandler = new TransactionHandler();
		ExecutionHistory executionHistory = new ExecutionHistory(entityClasses,
				transactionHandler);
		executionHistory.addWriteEntity(entity);
		// 2 - Submit it to the termination protocol.
		Future<TerminationResult> result = distributedTermination
				.terminateTransaction(executionHistory);
		result.get();
		// }
		writeRequestTime.stop();
	}

	public <E extends JessyEntity> void performNonTransactionalLocalWrite(
			E entity) throws InterruptedException, ExecutionException {
		writeRequestTime.start();
		dataStore.put(entity);
		lastCommittedEntities.put(entity.getKey(), entity);
		writeRequestTime.stop();
	}

	// FIXME Should this method be synchronized? I think it should only be
	// syncrhonized during certification. Thus, it is safe before certification
	// test.
	@Override
	public ExecutionHistory commitTransaction(
			TransactionHandler transactionHandler) {
		logger.info(transactionHandler + " IS COMMITTING");
		ExecutionHistory executionHistory = getExecutionHistory(transactionHandler);
		Future<TerminationResult> terminationResultFuture = distributedTermination
				.terminateTransaction(executionHistory);

		TerminationResult terminationResult = null;
		try {
			terminationResult = terminationResultFuture.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		assert (terminationResult != null);
		executionHistory.changeState(terminationResult.getTransactionState());
		
		logger.info(transactionHandler + " IS"
				+ terminationResult.getTransactionState());
		return executionHistory;
	}

	@Override
	public void open() {
		logger.info("Jessy is opened.");
		super.open();
	}

	public void close(Object object) {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.close(object);
		logger.info("Jessy is closed.");
		FractalManager.getInstance().stop();
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
