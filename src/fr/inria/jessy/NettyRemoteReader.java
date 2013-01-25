package fr.inria.jessy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import net.sourceforge.fractal.membership.Group;

import org.jboss.netty.channel.Channel;

import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.YCSBEntity;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.UnicastLearner;
import fr.inria.jessy.communication.UnicastClientManager;
import fr.inria.jessy.communication.UnicastServerManager;
import fr.inria.jessy.communication.message.ReadReplyMessage;
import fr.inria.jessy.communication.message.ReadRequestMessage;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;

/**
 * This class implements {@link RemoteReader} by using Netty package for
 * performing unicast operations.
 * 
 * @author Masoud Saeida Ardekani
 */

// TODO CAUTION: this implementation is not fault tolerant

public class NettyRemoteReader extends RemoteReader implements UnicastLearner {

	private BlockingQueue<ReadRequestMessage> requestQ;

	public UnicastClientManager cmanager;
	public UnicastServerManager smanager;

	public NettyRemoteReader(DistributedJessy j) {
		super(j);

		requestQ = new LinkedBlockingDeque<ReadRequestMessage>();

		if (JessyGroupManager.getInstance().isProxy()) {	
			cmanager = new UnicastClientManager(this,ConstantPool.JESSY_NETTY_REMOTE_READER_PORT, JessyGroupManager.getInstance()
					.getAllReplicaGroup().members());

			// The fastest way to handle client messages is to put them in a
			// queue, and only one thread tries to take them from the queue and
			// process them.

//			 pool.submitMultiple(new
//			 InnerObjectFactory<RemoteReadRequestTask>(
//			 RemoteReadRequestTask.class, NettyRemoteReader.class, this),4);

			pool.submit(new RemoteReadRequestTask());
		} else {
			smanager = new UnicastServerManager(this, ConstantPool.JESSY_NETTY_REMOTE_READER_PORT);

			// The fastest way to execute read requests at the server is to
			// handle requests in the same thread delivering them to the remote
			// reader. In other words, this approach is better than having a
			// separate thread handling them. Another advantage is that with
			// this approach, CPU can be consumed hundred percent.

			// pool.submitMultiple(new InnerObjectFactory<RemoteReadReplyTask>(
			// RemoteReadReplyTask.class, NettyRemoteReader.class, this),2);

			// pool.submit(new RemoteReadReplyTask());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E extends JessyEntity> Future<ReadReply<E>> remoteRead(
			ReadRequest<E> readRequest) throws InterruptedException {
		if (ConstantPool.logging)
			logger.debug("creating task for " + readRequest);
		
		RemoteReadFuture remoteRead = new RemoteReadFuture(readRequest);
		remoteReadQ.put(remoteRead);
		return remoteRead;
//		return sendRequest(readRequest);
	}
	
	private  <E extends JessyEntity> RemoteReadFuture sendRequest(ReadRequest<E> readRequest){

		long start = System.nanoTime();

		RemoteReadFuture remoteRead = new RemoteReadFuture(readRequest);
		

			Set<Group> dests = jessy.partitioner.resolve(readRequest);
			ArrayList<ReadRequest<JessyEntity>> toSend=new ArrayList<ReadRequest<JessyEntity>>(1);
			toSend.add((ReadRequest<JessyEntity>) readRequest);
			
			cmanager.unicast(
					new ReadRequestMessage(toSend), dests.iterator().next().members().iterator().next());
			
			pendingRemoteReads.put(readRequest.getReadRequestId(),
					remoteRead);
			
			return remoteRead;
	}

	@Override
	public void receiveMessage(Object message, Channel channel) {

		if (message instanceof ReadRequestMessage)
			learnReadRequestMessage((ReadRequestMessage)message, channel);
		else if  (message instanceof ReadReplyMessage)
			learnReadReplyMessage((ReadReplyMessage)message);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private  void learnReadRequestMessage(ReadRequestMessage readRequestMessage,
			Channel channel) {

		// try {
		// readRequestMessage.channel = channel;
		// requestQ.put(readRequestMessage);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }

		long start = System.nanoTime();

		List<ReadReply<JessyEntity>> replies = jessy.getDataStore().getAll(
				readRequestMessage.getReadRequests());
		// List<ReadReply<JessyEntity>>
		// replies=createFastYCSBReply(readRequestMessage.getReadRequests().get(0));

		start = System.nanoTime();
		channel.write(new ReadReplyMessage(replies));

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void learnReadReplyMessage(ReadReplyMessage msg) {

		List<ReadReply> list = msg.getReadReplies();
		batching.add(list.size());

		for (ReadReply reply : list) {

			logger.debug("reply " + reply.getReadRequestId());

			if (!pendingRemoteReads.containsKey(reply.getReadRequestId())) {
				logger.info("received an incorrect reply or request already served");
				continue;
			}

			if (pendingRemoteReads.get(reply.getReadRequestId()).mergeReply(
					reply))
				pendingRemoteReads.remove(reply.getReadRequestId());

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
						
						if (ConstantPool.logging)
							logger.debug("handling request" + rr.getReadRequestId());

						pendingRemoteReads.put(rr.getReadRequestId(),
								remoteRead);

						Set<Group> dests = jessy.partitioner.resolve(rr);

						for (Group dest : dests) {
							if (toSend.get(dest) == null)
								toSend.put(
										dest,
										new ArrayList<ReadRequest<JessyEntity>>(
												1));
							toSend.get(dest).add(rr);
						}

					}

					// Send them.
					for (Group dest : toSend.keySet()) {
						int swid = dest.getRandom();
						cmanager.unicast(
								new ReadRequestMessage(toSend.get(dest)), swid);
					}

					clientAskingTime.add(System.nanoTime() - start);

				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public class RemoteReadReplyTask implements Runnable {

		public RemoteReadReplyTask() {
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public void run() {

			while (true) {
				ReadRequestMessage readRequestMessage;
				try {
					readRequestMessage = requestQ.take();
					long start = System.nanoTime();
					List<ReadReply<JessyEntity>> replies = jessy.getDataStore()
							.getAll(readRequestMessage.getReadRequests());
					// List<ReadReply<JessyEntity>>
					// replies=createFastYCSBReply(readRequestMessage.getReadRequests().get(0));

					start = System.nanoTime();
					readRequestMessage.channel.write(new ReadReplyMessage(
							replies));

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

	}

	@SuppressWarnings("unused")
	private List<ReadReply<JessyEntity>> createFastYCSBReply(
			ReadRequest<JessyEntity> rr) {
		List<ReadReply<JessyEntity>> replies = new ArrayList<ReadReply<JessyEntity>>();

		String data = Utils.ASCIIString(1000);
		HashMap<String, String> tmp = new HashMap<String, String>();
		tmp.put("1", data);

		JessyEntity entity = new YCSBEntity(rr.getOneKey().getKeyValue()
				.toString(), tmp);

		ReadReply<JessyEntity> reply = new ReadReply<JessyEntity>(entity,
				rr.getReadRequestId());

		replies.add(reply);
		return replies;

	}
	
	@Override
	public void closeProxyConnections(){
			cmanager.close();
	}

	@Override
	public void closeReplicaConnections(){
			smanager.close();
	}

}
