package fr.inria.jessy;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.UMessage;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.rmcast.RMCastStream;
import net.sourceforge.fractal.rmcast.WanMessage;
import utils.ExecutorPool;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;

/**
 * 
 * A remote reader for distributed Jessy. This class takes as input a remote
 * read request via function
 * <p>
 * remoteRead
 * </p>
 * , and returns a Future encapsulating a JessyEntity. It maekes use of the
 * Fractal group ALLNODES to exchange replies , and create a
 * ReliableMulticastStream named RemoteReaderStream.
 * 
 * TODO: put the ExecutorPool inside Jessy (?) TODO: suppress or garbage-collect
 * cancelled requests.
 * 
 * @author Pierre Sutra
 * @author Masoud Saeida Ardekani
 */

public class RemoteReader implements Learner {

	private static RemoteReader instance;
	static {
		instance = new RemoteReader();
	}

	private ExecutorPool pool = ExecutorPool.getInstance();

	private RMCastStream stream;
	private ConcurrentMap<UUID, Future<ReadReply<? extends JessyEntity>>> futures;
	private ConcurrentMap<UUID, ReadReply<? extends JessyEntity>> replies;

	public static RemoteReader getInstance() {
		return instance;
	}

	private RemoteReader() {
		Membership.getInstance().getOrCreateTCPGroup("ALLNODES");
		stream = FractalManager.getInstance()
				.getOrCreateRMCastStream("RemoteReaderStream",
						Membership.getInstance().myGroup().name());
		stream.registerLearner("RemoteReadRequestMessage", this);
		stream.registerLearner("RemoteReadReplyMessage", this);
		futures = new ConcurrentHashMap<UUID, Future<ReadReply<? extends JessyEntity>>>();
		replies = new ConcurrentHashMap<UUID, ReadReply<? extends JessyEntity>>();
	}

	public <E extends JessyEntity> Future<ReadReply<E>> remoteRead(
			ReadRequest<E> readRequest) {
		assert !Partitioner.getInstance().isLocal(
				readRequest.getPartitioningKey());
		Future reply = pool.submit(new RemoteReadRequestTask(readRequest));
		futures.put(readRequest.getReadRequestId(), reply);
		return reply;
	}

	@SuppressWarnings("unchecked")
	@Deprecated
	public void learn(Stream s, Serializable v) {
		if (v instanceof RemoteReadRequestMessage) {
			pool.submit(new RemoteReadReplyTask((RemoteReadRequestMessage) v));
		} else { // RemoteReadReplyMessage
			ReadReply reply = ((RemoteReadReplyMessage) v).reply;
			replies.put(reply.getReadRequestId(), reply);
			futures.get(reply.getReadRequestId()).notify();
		}

	}

	public class RemoteReadReplyMessage extends UMessage {

		static final long serialVersionUID = ConstantPool.JESSY_MID;
		ReadReply reply;

		// For Fractal
		public RemoteReadReplyMessage() {
		}

		RemoteReadReplyMessage(ReadReply r) {
			super(r, Membership.getInstance().myId());
		}

	}

	public class RemoteReadRequestMessage extends WanMessage {

		private static final long serialVersionUID = ConstantPool.JESSY_MID;
		ReadRequest request;

		// For Fractal
		public RemoteReadRequestMessage() {
		}

		RemoteReadRequestMessage(ReadRequest r, Set<String> dest) {
			super(r, dest, Membership.getInstance().myGroup().name(),
					Membership.getInstance().myId());
		}

	}

	class RemoteReadRequestTask<E extends JessyEntity> implements
			Callable<ReadReply<E>> {

		private ReadRequest<E> request;

		private RemoteReadRequestTask(ReadRequest<E> readRequest) {
			this.request = readRequest;
		}

		public ReadReply<E> call() throws Exception {
			Set<String> dest = new HashSet<String>(1);
			dest.add(Partitioner.getInstance()
					.resolve(request.getPartitioningKey()).name());
			stream.reliableMulticast(new RemoteReadRequestMessage(request, dest));
			futures.get(request.getReadRequestId()).wait();
			return (ReadReply<E>) replies.get(request.getReadRequestId());
		}

	}

	class RemoteReadReplyTask implements Callable {

		private RemoteReadRequestMessage message;

		public RemoteReadReplyTask(RemoteReadRequestMessage m) {
			message = m;
		}

		public ReadReply call() throws Exception {
			ReadRequest readRequest = message.request;

			ReadReply readReply = DistributedJessy.getInstance().getDataStore()
					.get(readRequest);

			Membership
					.getInstance()
					.getOrCreateTCPGroup("ALLNODES")
					.unicastSW(message.source,
							new RemoteReadReplyMessage(readReply));
			return null;
		}

	}

}
