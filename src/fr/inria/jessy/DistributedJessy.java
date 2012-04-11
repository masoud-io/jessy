package fr.inria.jessy;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.sun.org.apache.xml.internal.security.keys.content.KeyValue;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.j5utils.utils.PerformanceProbe.SimpleCounter;

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

	private static final int REPLICATION_FACTOR = 1;

	private static DistributedJessy distributedJessy = null;
	private static DistributedTermination distributedTermination = null;
	private static RemoteReader remoteReader = null;

	private static SimpleCounter remoteCalls;

	static {
		try {

			// FIXME merge this in a PropertyHandler class (use Jean-Michel's
			// work).
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					"config.property");
			myProps.load(MyInputStream);
			String fractalFile = myProps.getProperty("fractal_file");
			MyInputStream.close();

			remoteCalls = new SimpleCounter("Jessy#DistantRemoteCalls");

			FractalManager.init(fractalFile);
			Membership.getInstance().dispatchPeers("J", REPLICATION_FACTOR);
			distributedJessy = new DistributedJessy();
			distributedTermination = new DistributedTermination(
					distributedJessy);
			remoteReader = new RemoteReader(distributedJessy);
			remoteReader.start();

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private DistributedJessy() throws Exception {
		super();
	}

	public static synchronized DistributedJessy getInstance() throws Exception {
		return distributedJessy;
	}

	@Override
	protected <E extends JessyEntity, SK> E performRead(Class<E> entityClass,
			String keyName, SK keyValue, CompactVector<String> readSet)
			throws InterruptedException, ExecutionException {
		System.out.print(keyValue+" IS ");
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keyName,
				keyValue, readSet);

		ReadReply<E> readReply;
		if (Partitioner.getInstance().isLocal(readRequest.getPartitioningKey())) {
			System.out.println("LOCAL READ");
			readReply = getDataStore().get(readRequest);
		} else {
			System.out.println("DISTANT READ");
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
		if (Partitioner.getInstance().isLocal(readRequest.getPartitioningKey())) {
			System.out.println("LOCAL READ");
			readReply = getDataStore().get(readRequest);
		} else {
			System.out.println("DISTANT READ");
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
		System.out.print(entity.getSecondaryKey()+" IS ");
		if (Partitioner.getInstance().isLocal(entity.getSecondaryKey()) && REPLICATION_FACTOR == 1) {
			System.out.println("LOCAL WRITE");
			performNonTransactionalLocalWrite(entity);
		} else {
			System.out.println("DISTRIBUTED WRITE");
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
	public void close() {
		// FIXME do a proper separation of client and server nodes.
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		super.close();
		remoteReader.stop();
	}

}
