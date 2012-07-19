package fr.inria.jessy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.ObjectUtils.InnerObjectFactory;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.ReadReplyMessage;
import fr.inria.jessy.communication.message.ReadRequestMessage;
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

// FIXME fix parametrized types.
// TODO CAUTION: this implementation is not fault tolerant

public class RemoteReader implements Learner {

	private static Logger logger = Logger.getLogger(RemoteReader.class);

	private static ValueRecorder batching,serverAnsweringTime, clientAskingTime, clientProcessingResponseTime;
	static {
		serverAnsweringTime = new ValueRecorder("RemoteReader#serverAnsweringTime(ms)");
		serverAnsweringTime.setFactor(1000000);
		serverAnsweringTime.setFormat("%a");
		
		clientAskingTime = new TimeRecorder("RemoteReader#clientAskingTime(ms)");
		clientAskingTime.setFactor(1000000);
		clientAskingTime.setFormat("%a");
		
		clientProcessingResponseTime = new TimeRecorder("RemoteReader#clientProcessingResponseTime(us)");
		clientProcessingResponseTime.setFactor(1000000);
		clientProcessingResponseTime.setFormat("%a");
		
		batching = new ValueRecorder("RemoteReader#batching)");
	}

	private Map<Integer, RemoteReadFuture<JessyEntity>> pendingRemoteReads;
	private BlockingQueue<RemoteReadFuture<JessyEntity>> remoteReadQ;

	private BlockingQueue<ReadRequestMessage> requestQ;

	private DistributedJessy jessy;
	private MulticastStream remoteReadStream;

	private ExecutorPool pool = ExecutorPool.getInstance();

	public RemoteReader(DistributedJessy j) {

		jessy = j;
		remoteReadStream = FractalManager.getInstance()
				.getOrCreateMulticastStream(
						ConstantPool.JESSY_READER_STREAM,
						JessyGroupManager.getInstance().getEverybodyGroup()
								.name());
		remoteReadStream.registerLearner("ReadRequestMessage", this);
		remoteReadStream.registerLearner("ReadReplyMessage", this);
		remoteReadStream.start();

		pendingRemoteReads = new ConcurrentHashMap<Integer, RemoteReadFuture<JessyEntity>>();

		requestQ = new LinkedBlockingDeque<ReadRequestMessage>();
		remoteReadQ = new LinkedBlockingDeque<RemoteReadFuture<JessyEntity>>();
		
		pool.submit(new RemoteReadReplyTask());
		pool.submit(new RemoteReadRequestTask());
		// With a LOW # of cores, contention is too expensive.
		// Besides, we are already batching.
		// pool.submitMultiple( new InnerObjectFactory<RemoteReadRequestTask>(RemoteReadRequestTask.class,
		// RemoteReader.class, this));
		// pool.submitMultiple(new InnerObjectFactory<RemoteReadReplyTask>(
		// RemoteReadReplyTask.class, RemoteReader.class, this));

	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> Future<ReadReply<E>> remoteRead(
			ReadRequest<E> readRequest) throws InterruptedException {
		logger.debug("creating task for " + readRequest);
		RemoteReadFuture remoteRead = new RemoteReadFuture(readRequest);
		remoteReadQ.put(remoteRead);
		return remoteRead;
	}

	@SuppressWarnings("unchecked")
	public void learn(Stream s, Serializable v) {
		
		if (v instanceof ReadRequestMessage) {
			
			try {
				requestQ.put((ReadRequestMessage) v);
			} catch (Exception e) {
				e.printStackTrace();
			}

		} else {

			long start = System.nanoTime();
			
			List<ReadReply> list = ((ReadReplyMessage) v).getReadReplies();
			batching.add(list.size());

			for (ReadReply reply : list) {

				logger.debug("reply " + reply.getReadRequestId());

				if (!pendingRemoteReads.containsKey(reply.getReadRequestId())) {
					logger.info("received an incorrect reply or request already served");
					continue;
				}

				if (pendingRemoteReads.get(reply.getReadRequestId())
						.mergeReply(reply))
					pendingRemoteReads.remove(reply.getReadRequestId());

			}

			clientProcessingResponseTime.add(System.nanoTime()-start);
			
		}
	}

	//
	// INNER CLASSES
	//

	public class RemoteReadFuture<E extends JessyEntity> implements
			Future<ReadReply<E>> {

		private Integer state; // 0 => init, 1 => done, -1 => cancelled
		private ReadReply<E> reply;
		private ReadRequest<E> readRequest;

		public RemoteReadFuture(ReadRequest<E> rr) {
			state = new Integer(0);
			reply = null;
			readRequest = rr;
		}

		public boolean cancel(boolean mayInterruptIfRunning) {
			synchronized (state) {
				if (state != 0)
					return false;
				state = -1;
				if (mayInterruptIfRunning)
					state.notifyAll();
			}
			return true;
		}

		public ReadReply<E> get() throws InterruptedException,
				ExecutionException {
			synchronized (state) {
				if (state == 0)
					state.wait();
			}
			return (state == -1) ? null : reply;
		}

		public ReadReply<E> get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			
			synchronized (state) {
				if (state == 0)
					state.wait(timeout);
			}
			return (state == -1) ? null : reply;
		}

		public boolean isCancelled() {
			return state == -1;
		}

		public boolean isDone() {
			return reply == null;
		}

		public boolean mergeReply(ReadReply<E> r) {

			synchronized (state) {

				if (state == -1)
					return true;

				if (reply == null) {
					reply = r;
				} else {
					reply.mergeReply(r);
				}

				if (readRequest.isOneKeyRequest()
						|| reply.getEntity().size() == readRequest
								.getMultiKeys().size()) {
					state.notifyAll();
					return true;
				}

				return false;
			}

		}

		public ReadRequest<E> getReadRequest() {
			return readRequest;
		}

	}

	public class RemoteReadRequestTask implements Runnable {

		private List<RemoteReadFuture<JessyEntity>> list;
		private Map<Group, List<ReadRequest<JessyEntity>>> toSend;

		public RemoteReadRequestTask() {
			list = new ArrayList<RemoteReadFuture<JessyEntity>>();
			toSend = new HashMap<Group, List<ReadRequest<JessyEntity>>>();

		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {

			try {

				while (true) {

					toSend.clear();
					list.clear();
					list.add(remoteReadQ.take());
					
					long start = System.nanoTime();
					
					remoteReadQ.drainTo(list);

					// Factorize read requests.
					for (RemoteReadFuture remoteRead : list) {

						ReadRequest<JessyEntity> rr = remoteRead
								.getReadRequest();
						logger.debug("handling request" + rr.getReadRequestId());

						pendingRemoteReads.put(rr.getReadRequestId(),
								remoteRead);

						Set<Group> dests = jessy.partitioner.resolve(rr);

						for (Group dest : dests) {
							if (toSend.get(dest) == null)
								toSend.put(
										dest,
										new ArrayList<ReadRequest<JessyEntity>>());
							toSend.get(dest).add(rr);
						}

					}

					// Send them.
					for (Group dest : toSend.keySet()) {
						int swid = dest.members().iterator().next(); // FIXME
																		// improve
																		// this.
						remoteReadStream.unicast(
								new ReadRequestMessage(toSend.get(dest)), swid);
					}
					
					clientAskingTime.add(System.nanoTime()-start);

				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public class RemoteReadReplyTask implements Runnable {

		private Map<Integer, List<ReadRequest<JessyEntity>>> pendingRequests;

		public RemoteReadReplyTask() {
			pendingRequests = new HashMap<Integer, List<ReadRequest<JessyEntity>>>();
		}

		@SuppressWarnings("unchecked")
		public void run() {
		
			Collection<ReadRequestMessage> msgs = new ArrayList<ReadRequestMessage>();
		
			while (true) {
		
				try {
		
					pendingRequests.clear();
					msgs.clear();
		
					msgs.add(requestQ.take());
					requestQ.drainTo(msgs);
					
					long start = System.nanoTime();
					
					for (ReadRequestMessage m : msgs) {
						if (!pendingRequests.containsKey(m.source)) {
							pendingRequests.put(m.source,
									new ArrayList<ReadRequest<JessyEntity>>());
						}
						pendingRequests.get(m.source).addAll(
								m.getReadRequests());
					}
		
					logger.debug("got" + pendingRequests);
		
					for (Integer dest : pendingRequests.keySet()) {
						List<ReadReply<JessyEntity>> replies = jessy
								.getDataStore().getAll(
										pendingRequests.get(dest));
						if (replies.isEmpty()) {
							logger.warn("read requests " + pendingRequests
									+ " failed");
						}
						
						remoteReadStream.unicast(new ReadReplyMessage(replies),
								dest);
					}
					
					serverAnsweringTime.add(System.nanoTime()-start);
		
				} catch (Exception e) {
					e.printStackTrace();
				}
		
			}
		}

	}

}
