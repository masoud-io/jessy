package fr.inria.jessy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.ObjectUtils.InnerObjectFactory;

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

public class NettyRemoteReader extends RemoteReader implements Learner {

	private BlockingQueue<ReadRequestMessage> requestQ;

	public UnicastClientManager cmanager;
	public UnicastServerManager smanager;
	
	
	public NettyRemoteReader(DistributedJessy j) {
		super(j);

		requestQ = new LinkedBlockingDeque<ReadRequestMessage>();
		
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
	@Override
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
		
		if (!(v instanceof ReadRequestMessage)) {			

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
