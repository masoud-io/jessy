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
import net.sourceforge.fractal.utils.PerformanceProbe.SimpleCounter;

import org.apache.log4j.Logger;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.termination.DistributedTermination;
import fr.inria.jessy.transaction.termination.TerminationResult;
import fr.inria.jessy.vector.CompactVector;

public class DistributedJessy extends Jessy {
	
	private static Logger logger = Logger.getLogger(DistributedJessy .class);
	private static DistributedJessy distributedJessy = null;
	
	private ConstantPool.EXECUTION_MODE mode = ConstantPool.EXECUTION_MODE.PROXY; 

	public FractalManager fractal;
	public Membership membership; 
	public RemoteReader remoteReader;
	public DistributedTermination distributedTermination;
	public Partitioner partitioner;
	
	private static SimpleCounter remoteCalls;

	private DistributedJessy() throws Exception {
		super();
		try {
			
			// FIXME move this.
			// Merge it in a PropertyHandler class (use Jean-Michel's work).
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream("config.property");
			myProps.load(MyInputStream);
			String fractalFile = myProps.getProperty("fractal_file");
			String executionMode = myProps.getProperty("execution_mode");
			if(executionMode.equals("server")) mode = ConstantPool.EXECUTION_MODE.SERVER;
			MyInputStream.close();

			// Initialize Fractal
			fractal = FractalManager.init(fractalFile);
			membership = fractal.membership;

			// Create Jessy server groups
			if(mode.equals(ConstantPool.EXECUTION_MODE.SERVER)){
				membership.dispatchPeers(ConstantPool.JESSY_SERVER_GROUP, ConstantPool.JESSY_SERVER_PORT, ConstantPool.REPLICATION_FACTOR);
				distributedTermination = new DistributedTermination(this,fractal.membership.myGroup());
				partitioner = new Partitioner(membership);
			}else{

			}

			// Create Jessy global group
			Group g = fractal.membership.getOrCreateTCPDynamicGroup(ConstantPool.JESSY_ALL_GROUP, ConstantPool.JESSY_ALL_PORT);
			g.putNodes(fractal.membership.allNodes());

			remoteReader = new RemoteReader(this);
			remoteReader.start();

			// Logging facilities
			remoteCalls = new SimpleCounter("Jessy#DistantRemoteCalls");

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static synchronized DistributedJessy getInstance() {
		if(distributedJessy==null){
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

		ReadReply<E> readReply;
		if (partitioner.isLocal(readRequest.getPartitioningKey())) {
			logger.debug("Performing Local Read for: " + keyValue);
			readReply = getDataStore().get(readRequest);
		} else {
			logger.debug("Performing Remote Read for: " + keyValue);
			remoteCalls.incr();
			Future<ReadReply<E>> future = remoteReader.remoteRead(readRequest);
			readReply = future.get();
		}

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
		System.out.print("GOT");
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keys,
				readSet);

		ReadReply<E> readReply;
		if (partitioner.isLocal(readRequest.getPartitioningKey())) {
			logger.debug("Performing Local Read for: " + keys);
			readReply = getDataStore().get(readRequest);
		} else {
			logger.debug("Performing Remote Read for: " + keys);
			remoteCalls.incr();
			Future<ReadReply<E>> future = remoteReader.remoteRead(readRequest);
			readReply = future.get();
		}

		if (readReply.getEntity().iterator().hasNext())
			return readReply.getEntity();
		else
			return null;
	}

	@Override
	public <E extends JessyEntity> void performNonTransactionalWrite(E entity)
			throws InterruptedException, ExecutionException {
		if (partitioner.isLocal(entity.getSecondaryKey())
				&& ConstantPool.REPLICATION_FACTOR == 1) {
			logger.debug("Performing Local Write for: " + entity.getSecondaryKey());
			performNonTransactionalLocalWrite(entity);
		} else {
			logger.debug("Performing Distributed Write for: " + entity.getSecondaryKey());
			remoteCalls.incr();
			// 1 - Create a blind write transaction.
			TransactionHandler transactionHandler = new TransactionHandler();
			ExecutionHistory executionHistory = new ExecutionHistory(
					entityClasses, transactionHandler);
			executionHistory.addWriteEntity(entity);
			// 2 - Submit it to the termination protocol.
			Future<TerminationResult> result = distributedTermination
					.terminateTransaction(executionHistory);
			result.get();
		}
	}

	public <E extends JessyEntity> void performNonTransactionalLocalWrite(
			E entity) throws InterruptedException, ExecutionException {
		dataStore.put(entity);
	}

	// FIXME Should this method be synchronized? I think it should only be
	// syncrhonized during certification. Thus, it is safe before certification
	// test.
	@Override
	public ExecutionHistory commitTransaction(
			TransactionHandler transactionHandler) {
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
		return executionHistory;
	}

	@Override
	public void close(Object object) {
		// FIXME do a proper separation of client and server nodes.
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		super.close(object);
		remoteReader.stop();
	}

}
