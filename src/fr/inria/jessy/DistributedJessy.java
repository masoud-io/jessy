package fr.inria.jessy;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.membership.Membership;

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

	private static DistributedJessy distributedJessy = null;
	private static DistributedTermination distributedTermination = null;
		

	static {
		try {
			FractalManager.init("myfractal.xml");
			distributedJessy = new DistributedJessy();
			distributedTermination = new DistributedTermination(distributedJessy);
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
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keyName,
				keyValue, readSet);

		ReadReply<E> readReply;
//		if (Partitioner.getInstance().isLocal(readRequest.getPartitioningKey())) {
//			readReply = getDataStore().get(readRequest);
//		} else {
			Future<ReadReply<E>> future = RemoteReader.getInstance()
					.remoteRead(readRequest);
			readReply = future.get();
//		}
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
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keys,
				readSet);

		ReadReply<E> readReply;
		if (Partitioner.getInstance().isLocal(readRequest.getPartitioningKey())) {
			readReply = getDataStore().get(readRequest);

		} else {
			Future<ReadReply<E>> future = RemoteReader.getInstance()
					.remoteRead(readRequest);
			readReply = future.get();
		}

		if (readReply.getEntity().iterator().hasNext())
			return readReply.getEntity();
		else
			return null;
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

}
