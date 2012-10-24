package fr.inria.jessy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.multicast.MulticastStream;

import com.yahoo.ycsb.YCSBEntity;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.ReadReplyMessage;
import fr.inria.jessy.communication.message.ReadRequestMessage;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;

/**
 * 
 * This class implements {@link RemoteReader} by using Netty package for
 * performing unicast operations.
 * 
 * @author Pierre Sutra
 * @author Masoud Saeida Ardekani
 */

// FIXME fix parametrized types.
// TODO CAUTION: this implementation is not fault tolerant

public class FractalRemoteReader extends RemoteReader implements Learner {

	private BlockingQueue<ReadRequestMessage> requestQ;

	private MulticastStream remoteReadStream;

	public FractalRemoteReader(DistributedJessy j) {
		super(j);
		remoteReadStream = FractalManager.getInstance()
				.getOrCreateMulticastStream(
						ConstantPool.JESSY_READER_STREAM,
						JessyGroupManager.getInstance().getEverybodyGroup()
								.name());
		remoteReadStream.registerLearner("ReadRequestMessage", this);
		remoteReadStream.registerLearner("ReadReplyMessage", this);
		remoteReadStream.start();

		requestQ = new LinkedBlockingDeque<ReadRequestMessage>();

		pool.submit(new RemoteReadReplyTask());
		pool.submit(new RemoteReadRequestTask());
		// With a LOW # of cores, contention is too expensive.
		// Besides, we are already batching.
		// pool.submitMultiple( new
		// InnerObjectFactory<RemoteReadRequestTask>(RemoteReadRequestTask.class,
		// RemoteReader.class, this));
		// pool.submitMultiple(new InnerObjectFactory<RemoteReadReplyTask>(
		// RemoteReadReplyTask.class, RemoteReader.class, this));
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

					clientAskingTime.add(System.nanoTime() - start);

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
					long start = System.nanoTime();

					pendingRequests.clear();
					msgs.clear();

					msgs.add(requestQ.take());
					requestQ.drainTo(msgs);

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

					serverAnsweringTime.add(System.nanoTime() - start);

				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}

	}

	private ReadReply<JessyEntity> createFastYCSBReply(
			ReadRequest<JessyEntity> rr) {
		JessyEntity entity = new YCSBEntity(rr.getOneKey().getKeyValue()
				.toString());
		ReadReply<JessyEntity> reply = new ReadReply<JessyEntity>(entity,
				rr.getReadRequestId());
		return reply;

	}

}
