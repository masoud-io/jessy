package fr.inria.jessy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.MessageOutputStream;
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
	private Map<UUID, ReadReply<? extends JessyEntity>> replies;
	private Map<UUID, ReadRequest<? extends JessyEntity>> requests;

	public static RemoteReader getInstance() {
		return instance;
	}

	private RemoteReader() {
		stream = FractalManager.getInstance()
				.getOrCreateRMCastStream("RemoteReaderStream",
						Membership.getInstance().myGroup().name());
		stream.registerLearner("RemoteReadRequestMessage", this);
		stream.registerLearner("RemoteReadReplyMessage", this);
		stream.start();
		replies = new ConcurrentHashMap<UUID, ReadReply<? extends JessyEntity>>();
		requests = new ConcurrentHashMap<UUID, ReadRequest<? extends JessyEntity>>();
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> Future<ReadReply<E>> remoteRead(
			ReadRequest<E> readRequest) {
//		assert !Partitioner.getInstance().isLocal(
//				readRequest.getPartitioningKey());
		requests.put(readRequest.getReadRequestId(),readRequest);
		Future reply = pool.submit(new RemoteReadRequestTask(readRequest));
		return reply;
	}

	@SuppressWarnings("unchecked")
	@Deprecated
	public void learn(Stream s, Serializable v) {
		if (v instanceof RemoteReadRequestMessage) {
			pool.submit(new RemoteReadReplyTask((RemoteReadRequestMessage) v));
		} else { // RemoteReadReplyMessage
			ReadReply reply = ((RemoteReadReplyMessage) v).getReadReply();
			replies.put(reply.getReadRequestId(), reply);
			synchronized(requests.get(reply.getReadRequestId())){
				requests.get(reply.getReadRequestId()).notify();
			}
		}

	}

	class RemoteReadRequestTask<E extends JessyEntity> implements
			Callable<ReadReply<E>> {

		private ReadRequest<E> request;

		private RemoteReadRequestTask(ReadRequest<E> readRequest) {
			this.request = readRequest;
		}

		public ReadReply<E> call() throws Exception {
			// TODO clean 
			Set<String> dest = new HashSet<String>(1);
			dest.add(Partitioner.getInstance().resolve(
					request.getPartitioningKey()).name());
			stream.reliableMulticast(new RemoteReadRequestMessage(request, dest));  // FIXME unicast to the right guy ?
			synchronized(requests.get(request.getReadRequestId())){
				requests.get(request.getReadRequestId()).wait();
			}
			ReadReply<E> reply = (ReadReply<E>) replies.get(request.getReadRequestId());
			return reply;
		}

	}

	class RemoteReadReplyTask implements Callable {

		private RemoteReadRequestMessage message;

		public RemoteReadReplyTask(RemoteReadRequestMessage m) {
			message = m;
		}

		public ReadReply call() throws Exception {
			ReadRequest readRequest = message.getReadRequest();

			ReadReply readReply = DistributedJessy.getInstance().getDataStore()
					.get(readRequest);
			
			Set<String> d = new HashSet<String>();
			d.add(message.gSource);
			stream.reliableMulticast(new RemoteReadReplyMessage(readReply,d)); // FIXME unicast to the right guy
			return null;
		}

	}

}
