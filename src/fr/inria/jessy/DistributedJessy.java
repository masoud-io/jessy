package fr.inria.jessy;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.rmcast.RMCastStream;
import net.sourceforge.fractal.rmcast.WanMessage;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import utils.ExecutorPool;
import fr.inria.jessy.RemoteReader.RemoteReadReplyMessage;
import fr.inria.jessy.RemoteReader.RemoteReadReplyTask;
import fr.inria.jessy.RemoteReader.RemoteReadRequestMessage;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.CompactVector;

public class DistributedJessy extends Jessy {

	private static DistributedJessy distributedJessy = null;
	private ExecutorPool pool = ExecutorPool.getInstance();
	private WanAMCastStream amStream;

	private DistributedJessy() throws Exception {
		super();

		amStream = FractalManager.getInstance().getOrCreateWanAMCastStream(
				"DistributedJessy", Membership.getInstance().myGroup().name());
		amStream.registerLearner("TerminateTransactionMessage", this);
	}

	public static synchronized DistributedJessy getInstance() throws Exception {
		if (distributedJessy == null) {
			distributedJessy = new DistributedJessy();
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
		if (Partitioner.getInstance().isLocal(readRequest.getPartitioningKey())) {

			readReply = getDataStore().get(readRequest);

		} else {
			Future<ReadReply<E>> future = RemoteReader.getInstance()
					.remoteRead(readRequest);
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

	@Override
	// FIXME Should this method be synchronized? I think it should only be
	// syncrhonized during certification. Thus, it is safe before certification
	// test.
	public boolean terminateTransaction(TransactionHandler transactionHandler) {

		return false;
	}
	
	@SuppressWarnings("unchecked")
	@Deprecated
	public void learn(Stream s, Serializable v) {
		if(v instanceof TerminateTransactionMessage){
			pool.submit(new RemoteReadReplyTask((RemoteReadRequestMessage)v));
		} 
			
	}
	
	class  TerminateTransactionTask<E extends JessyEntity> implements Callable<ReadReply<E>>{
		
		private ReadRequest<E> request;
		
		private RemoteReadRequestTask(ReadRequest<E> readRequest){
			this.request=readRequest;
		}
		
		public ReadReply<E> call() throws Exception {
			Set<String> dest = new HashSet<String>(1);
			dest.add(Partitioner.getInstance().resolve(request.getPartitioningKey()).name());
			stream.reliableMulticast(new RemoteReadRequestMessage(request,dest));
			replies.get(request.getReadRequestId()).wait();
			return (ReadReply<E>) replies.get(request.getReadRequestId());
		}
		
	}

	public class TerminateTransactionMessage extends WanMessage {

		private static final long serialVersionUID = ConstantPool.JESSY_MID;
		ExecutionHistory executionHistory;

		// For Fractal
		public TerminateTransactionMessage() {}
		
		TerminateTransactionMessage(ExecutionHistory eh, Set<String> dest) {
			super(eh, dest, Membership.getInstance().myGroup().name(),Membership.getInstance().myId());
		}
	
	}
}
