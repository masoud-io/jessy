package fr.inria.jessy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
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
import org.cliffc.high_scale_lib.NonBlockingHashtable;
import org.jboss.netty.channel.Channel;

import com.yahoo.ycsb.YCSBEntity;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.UnicastClientManager;
import fr.inria.jessy.communication.UnicastServerManager;
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

public class NettyRemoteReader implements Learner {

	private static Logger logger = Logger.getLogger(NettyRemoteReader.class);

	private static ValueRecorder batching_ReadRequest,batching,serverLookupTime,serverSendingTime,serverAnsweringTime, clientAskingTime;
	static {
		serverLookupTime = new ValueRecorder("RemoteReader#serverLookupTime(ms)");
		serverLookupTime.setFactor(1000000);
		serverLookupTime.setFormat("%a");

		serverSendingTime = new ValueRecorder("RemoteReader#serverSendingTime(ms)");
		serverSendingTime.setFactor(1000000);
		serverSendingTime.setFormat("%a");

		serverAnsweringTime = new ValueRecorder("RemoteReader#serverAnsweringTime(ms)");
		serverAnsweringTime.setFactor(1000000);
		serverAnsweringTime.setFormat("%a");

		
		clientAskingTime = new TimeRecorder("RemoteReader#clientAskingTime(ms)");
		clientAskingTime.setFactor(1000000);
		clientAskingTime.setFormat("%a");
		
		batching = new ValueRecorder("RemoteReader#batching)");
		batching_ReadRequest= new ValueRecorder("RemoteReader#batching_ReadRequest)");
	}

	private NonBlockingHashtable<Integer, RemoteReadFuture<JessyEntity>> pendingRemoteReads;
	
	private BlockingQueue<RemoteReadFuture<JessyEntity>> remoteReadQ;

	private BlockingQueue<ReadRequestMessage> requestQ;

	private DistributedJessy jessy;
	private MulticastStream remoteReadStream;

	private ExecutorPool pool = ExecutorPool.getInstance();

	public UnicastClientManager cmanager;
	public UnicastServerManager smanager;
	
	
	public NettyRemoteReader(DistributedJessy j) {
		jessy = j;
		remoteReadStream = FractalManager.getInstance()
				.getOrCreateMulticastStream(
						ConstantPool.JESSY_READER_STREAM,
						JessyGroupManager.getInstance().getEverybodyGroup()
								.name());
		remoteReadStream.registerLearner("ReadRequestMessage", this);
		remoteReadStream.registerLearner("ReadReplyMessage", this);
		remoteReadStream.start();

		pendingRemoteReads = new NonBlockingHashtable<Integer, RemoteReadFuture<JessyEntity>>();

		requestQ = new LinkedBlockingDeque<ReadRequestMessage>();
		remoteReadQ = new LinkedBlockingDeque<RemoteReadFuture<JessyEntity>>();
		
//		pool.submit(new RemoteReadReplyTask());
//		pool.submit(new RemoteReadRequestTask());
		// With a LOW # of cores, contention is too expensive.
		// Besides, we are already batching.
		 pool.submitMultiple( new InnerObjectFactory<RemoteReadRequestTask>(RemoteReadRequestTask.class,
		 NettyRemoteReader.class, this));
//		 pool.submitMultiple(new InnerObjectFactory<RemoteReadReplyTask>(
//		 RemoteReadReplyTask.class, RemoteReader.class, this));

		if (JessyGroupManager.getInstance().isProxy())
			cmanager=new UnicastClientManager(this);
		else 
			smanager=new UnicastServerManager(this);
	}

	
	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> Future<ReadReply<E>> remoteRead(
			ReadRequest<E> readRequest) throws InterruptedException {
		logger.debug("creating task for " + readRequest);
		RemoteReadFuture remoteRead = new RemoteReadFuture(readRequest);
		remoteReadQ.put(remoteRead);
		return remoteRead;
	}

	public void learnReadRequestMessage(ReadRequestMessage readRequestMessage, Channel channel){
//		
//		try {
//			readRequestMessage.channel=channel;
//			requestQ.put(readRequestMessage);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		long start = System.nanoTime();
			List<ReadReply<JessyEntity>> replies = jessy
					.getDataStore().getAll(readRequestMessage.getReadRequests());
			
			serverLookupTime.add(System.nanoTime()-start);
			
			start=System.nanoTime();
			channel.write(new ReadReplyMessage(replies));

			serverSendingTime.add(System.nanoTime()-start);
	}
	
	@SuppressWarnings("unchecked")
	public void learn(Stream s, Serializable v) {
		
		if (v instanceof ReadRequestMessage) {
			
			try {
				requestQ.put((ReadRequestMessage) v);
				
//				{
//					long start = System.nanoTime();
//					
//					ReadRequestMessage  tmp=(ReadRequestMessage) v;
//					
//						List<ReadReply<JessyEntity>> replies = jessy
//								.getDataStore().getAll(tmp.getReadRequests());
//						
//						serverLookupTime.add(System.nanoTime()-start);
//						
//						start=System.nanoTime();
//						smanager.unicast(new ReadReplyMessage(replies),
//								tmp.source);
////						remoteReadStream.unicast(new ReadReplyMessage(replies),
////								tmp.source);
//
//						serverSendingTime.add(System.nanoTime()-start);
//				}
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

//			clientProcessingResponseTime.add(System.nanoTime()-start);
			
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
						cmanager.unicast(new ReadRequestMessage(toSend.get(dest)), swid);
//						remoteReadStream.unicast(
//								new ReadRequestMessage(toSend.get(dest)), swid);
					}
					
					clientAskingTime.add(System.nanoTime()-start);

				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public class RemoteReadReplyTask implements Runnable {

//		private Map<Integer, List<ReadRequest<JessyEntity>>> pendingRequests;

		public RemoteReadReplyTask() {
//			pendingRequests = new HashMap<Integer, List<ReadRequest<JessyEntity>>>();
		}

		@SuppressWarnings("unchecked")
		public void run() {
		
			while (true){				
				ReadRequestMessage readRequestMessage;
				try {
					readRequestMessage = requestQ.take();
					List<ReadReply<JessyEntity>> replies = jessy
							.getDataStore().getAll(readRequestMessage.getReadRequests());
					
					readRequestMessage.channel.write(new ReadReplyMessage(replies));
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
	
		
//			Collection<ReadRequestMessage> msgs = new ArrayList<ReadRequestMessage>();
//		
//			while (true) {
//		
//				try {
//					long start = System.nanoTime();
//		
//					pendingRequests.clear();
//					msgs.clear();
//		
//					msgs.add(requestQ.take());
//					requestQ.drainTo(msgs);
//					
//					
//					for (ReadRequestMessage m : msgs) {
//						if (!pendingRequests.containsKey(m.source)) {
//							pendingRequests.put(m.source,
//									new ArrayList<ReadRequest<JessyEntity>>());
//						}
//						pendingRequests.get(m.source).addAll(
//								m.getReadRequests());
//					}
//		
//					logger.debug("got" + pendingRequests);
//		
//					for (Integer dest : pendingRequests.keySet()) {
//						batching_ReadRequest.add(pendingRequests.get(dest).size());
//						List<ReadReply<JessyEntity>> replies = jessy
//								.getDataStore().getAll(
//										pendingRequests.get(dest));
//						if (replies.isEmpty()) {
//							logger.warn("read requests " + pendingRequests
//									+ " failed");
//						}
//						
////						smanager.unicast(new ReadReplyMessage(replies),
////								dest);
////						remoteReadStream.unicast(new ReadReplyMessage(replies),
////								dest);
//					}
//					
//					serverAnsweringTime.add(System.nanoTime()-start);
//		
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//		
//			}
		}

	}
	
	private ReadReply<JessyEntity> createFastYCSBReply(ReadRequest<JessyEntity> rr){
		JessyEntity entity=new YCSBEntity(rr.getOneKey().getKeyValue().toString());
		ReadReply<JessyEntity> reply=new ReadReply<JessyEntity>(entity,rr.getReadRequestId());
		return reply;
		
	}

}
