package fr.inria.jessy.transaction.termination;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;
import static fr.inria.jessy.transaction.TransactionState.COMMITTED;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastMessage;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;

import org.apache.log4j.Logger;

import com.sun.corba.se.spi.legacy.connection.GetEndPointInfoAgainException;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionReplyMessage;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.termination.message.VoteMessage;
import fr.inria.jessy.utils.ExecutorPool;

//TODO COMMENT ME
//TODO Clean these ConcurrentMaps
public class DistributedTermination implements Learner, Runnable {

	private static Logger logger = Logger
			.getLogger(DistributedTermination.class);

	private DistributedJessy jessy;

	private ExecutorPool pool = ExecutorPool.getInstance();

	private WanAMCastStream atomicMulticastStream;
	private MulticastStream voteStream;
	private MulticastStream terminationNotificationStream;

	private Membership membership;
	private Group group;

	private Map<UUID, TransactionHandler> terminationRequests;
	private Map<UUID, TerminationResult> terminationResults;
	private Map<TransactionHandler, VotingQuorum> votingQuorums;
	private Map<TransactionHandler, String> coordinatorGroups;

	private LinkedBlockingQueue<TerminateTransactionRequestMessage> atomicDeliveredMessages;
	private Map<TransactionHandler, TerminateTransactionRequestMessage> processingMessages;

	private List<TransactionHandler> terminated;
	private Thread thread;

	public DistributedTermination(DistributedJessy j, Group g) {

		jessy = j;
		membership = j.membership;
		group = g;

		terminationNotificationStream = FractalManager.getInstance()
				.getOrCreateMulticastStream(ConstantPool.JESSY_ALL_GROUP,
						ConstantPool.JESSY_ALL_GROUP);
		terminationNotificationStream.registerLearner(
				"TerminateTransactionReplyMessage", this);

		voteStream = FractalManager.getInstance().getOrCreateMulticastStream(
				group.name(), group.name());
		voteStream.registerLearner("VoteMessage", this);

		atomicMulticastStream = FractalManager.getInstance()
				.getOrCreateWanAMCastStream(group.name(), group.name());
		atomicMulticastStream.registerLearner(
				"TerminateTransactionRequestMessage", this);

		terminationNotificationStream.start();
		voteStream.start();
		atomicMulticastStream.start();

		terminationRequests = new ConcurrentHashMap<UUID, TransactionHandler>();
		terminationResults = new ConcurrentHashMap<UUID, TerminationResult>();
		atomicDeliveredMessages = new LinkedBlockingQueue<TerminateTransactionRequestMessage>();
		processingMessages = new ConcurrentHashMap<TransactionHandler, TerminateTransactionRequestMessage>();
		votingQuorums = new ConcurrentHashMap<TransactionHandler, VotingQuorum>();
		coordinatorGroups = new ConcurrentHashMap<TransactionHandler, String>();

		terminated = new CopyOnWriteArrayList<TransactionHandler>();

		thread = new Thread(this);
		thread.start();
	}

	public Future<TerminationResult> terminateTransaction(ExecutionHistory ex) {

		logger.debug("terminate transaction "
				+ ex.getTransactionHandler().getId());
		ex.changeState(TransactionState.COMMITTING);
		terminationRequests.put(ex.getTransactionHandler().getId(),
				ex.getTransactionHandler());
		Future<TerminationResult> reply = pool.submit(new AtomicMulticastTask(
				ex));
		return reply;
	}

	@Deprecated
	public void learn(Stream s, Serializable v) {

		if (v instanceof TerminateTransactionRequestMessage) {

			TerminateTransactionRequestMessage requestMessage = (TerminateTransactionRequestMessage) v;
			atomicDeliveredMessages.add(requestMessage);
			coordinatorGroups.put(requestMessage.getExecutionHistory()
					.getTransactionHandler(), requestMessage.gSource);

		} else if (v instanceof TerminateTransactionReplyMessage) {

			TerminateTransactionReplyMessage replyMessage = (TerminateTransactionReplyMessage) v;
			TransactionHandler th = replyMessage.getTerminationResult()
					.getTransactionHandler();

			logger.debug("got a terminateTransactionReplyMesssage for "
					+ th.getId());

			if (terminated.contains(th))
				return;

			/*
			 * This node is the coordinator, and it is not in the replica set.
			 */
			assert terminationRequests.containsKey(th.getId());
			assert replyMessage.getTerminationResult()
					.isSendBackToCoordinator();

			try {
				handleTerminationResult(null,
						replyMessage.getTerminationResult());
			} catch (Exception e) {
				e.printStackTrace();
			}

		} else { // VoteMessage

			Vote vote = ((VoteMessage) v).getVote();
			if (terminated.contains(vote.getTransactionHandler()))
				return;
			addVote(vote);

		}

	}

	/**
	 * Continuesly, checks {@code atomicDeliveredMessages} queue, and if the
	 * message in the head of the queue does not conflict with already
	 * committing transactions, creates a new {@code CertifyAndVoteTask} for
	 * processing it. Otherwise, waits until one message in processingMessages
	 * list finishes its execution.
	 * 
	 */
	@Override
	public void run() {
		TerminateTransactionRequestMessage terminateRequestMessage;
		TerminateTransactionRequestMessage processingMessage;

		while (true) {

			/*
			 * When a new certify and vote task is submitted, it is first check
			 * whether is a concurrent conflicting transaction already
			 * committing or not. If not, the message is put in {@code
			 * processingMessages} map, otherwise, it should wait until one
			 * transaction finishes its termination.
			 */
			try {
				terminateRequestMessage = atomicDeliveredMessages.take();
				Iterator<TerminateTransactionRequestMessage> itr = processingMessages
						.values().iterator();
				while (itr.hasNext()) {
					processingMessage = itr.next();
					if (ConsistencyFactory.getConsistency().hasConflict(
							terminateRequestMessage.getExecutionHistory(),
							processingMessage.getExecutionHistory())) {
						synchronized (processingMessage) {
							processingMessage.wait();
						}
						itr = processingMessages.values().iterator();
						continue;
					}
				}
				processingMessages.put(terminateRequestMessage
						.getExecutionHistory().getTransactionHandler(),
						terminateRequestMessage);

				/*
				 * There is no conflict with already processing messages in
				 * {@code processingMessages}, thus a new {@code
				 * CertifyAndVoteTask} is created for handling this messages.
				 */
				pool.submit(new CertifyAndVoteTask(terminateRequestMessage));
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}

		}

	}

	/**
	 * Upon receiving a new certification vote, it is added to the
	 * votingQuorums.
	 * 
	 * @param vote
	 */
	private void addVote(Vote vote) {
		if (votingQuorums.containsKey(vote.getTransactionHandler())) {
			votingQuorums.get(vote.getTransactionHandler()).addVote(vote);
		} else {
			VotingQuorum vq = new VotingQuorum(vote.getTransactionHandler(),
					vote.getAllVoterGroups());
			vq.addVote(vote);
			votingQuorums.put(vote.getTransactionHandler(), vq);
		}
	}

	/**
	 * If this method is called at the transaction's coordinator, we should
	 * notify the pending future of the coordinator. Otherwise, the method is
	 * called at a remote replica. If the transaction is not going to be
	 * certified at the coordinator (coordinator does not replicate an object
	 * concerned by the transaction), the {@code terminationCode} should be send
	 * to the coordinator.
	 * 
	 * PIERRE null for executionHistory indicates that we are at the coordinator
	 * and the coordinator is not in the replica set
	 * 
	 * FIXME change this
	 * 
	 * TODO should it be synchronized?
	 */
	private void handleTerminationResult(ExecutionHistory executionHistory,
			TerminationResult terminationResult) throws Exception {

		logger.debug("handling termination result for "
				+ terminationResult.getTransactionHandler().getId());

		if(terminated.contains(terminationResult.getTransactionHandler()))
			return;
		
		HashSet<String> cordinatorGroup = new HashSet<String>();
		cordinatorGroup.add(coordinatorGroups.get(terminationResult
				.getTransactionHandler()));

		/*
		 * Apply the transaction if (i) it is committed and (ii) we hold a
		 * modified entity. Observe that if executionHistory==null, then we are
		 * the coord. and we own no modified entity.
		 */
		if (terminationResult.getTransactionState() == COMMITTED
				&& executionHistory != null) {
			boolean hasLocal = false;
			for (JessyEntity e : executionHistory.getWriteSet().getEntities()) {
				if (jessy.partitioner.isLocal(e.getKey())) {
					try {
						hasLocal = true;
						jessy.performNonTransactionalLocalWrite(e);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
			}
			for (JessyEntity e : executionHistory.getCreateSet().getEntities()) {
				if (jessy.partitioner.isLocal(e.getKey())) {
					try {
						hasLocal = true;
						jessy.performNonTransactionalLocalWrite(e);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
			}
			assert hasLocal;
		}

		/*
		 * if the future is not null, it means that this process is the
		 * coordinator, and it has received the outcome of a transaction
		 * termination phase. It puts the result in the terminationResults Map,
		 * and call the notify the future to finishes its task.
		 * 
		 * Otherwise, a non-cordinator has received the required votes, and has
		 * decided the outcome of the transaction. So, if the coordinator does
		 * not receive the outcome, this process unicasts the result to the
		 * coordinator.
		 */
		if (terminationRequests.containsKey(terminationResult
				.getTransactionHandler().getId())) {
			TransactionHandler transactionHandler = terminationRequests
					.get(terminationResult.getTransactionHandler().getId());
			logger.debug("at the coordinator, notifying for "
					+ transactionHandler.getId());
			synchronized (transactionHandler) {
				terminationResults.put(terminationResult
						.getTransactionHandler().getId(), terminationResult);
				transactionHandler.notify();
			}
		} else if (terminationResult.isSendBackToCoordinator()) {
			MulticastMessage replyMessage = new TerminateTransactionReplyMessage(
					terminationResult, cordinatorGroup, group.name(),
					membership.myId());

			logger.debug("send a terminateTransactionReplyMesssage for "
					+ executionHistory.getTransactionHandler().getId());
			terminationNotificationStream.unicast(replyMessage,
					executionHistory.getCoordinator());

		}

		garbageCollect(terminationResult.getTransactionHandler());
	}

	/**
	 * Garbage collect all concurrent hash maps entries for the given
	 * {@code transactionHandler}
	 * 
	 * TODO is it necessary to be synchronized?
	 * 
	 * @param transactionHandler
	 *            The transactionHandler to be garbage collected.
	 */
	private void garbageCollect(TransactionHandler transactionHandler) {
		
		/*
		 * Upon removing the transaction from {@code processingMessages}, it
		 * notifies the main thread to process waiting messages again.
		 */
		if(processingMessages.containsKey(transactionHandler)){
			TerminateTransactionRequestMessage terminatedMessage = processingMessages
			.get(transactionHandler);

			processingMessages.remove(transactionHandler);
			synchronized (terminatedMessage) {
				terminatedMessage.notify();
			}
		}

		terminated.add(transactionHandler);

		if (terminationRequests.containsKey(transactionHandler.getId())) {
			logger.debug("garbage-collect for " + transactionHandler.getId());
			terminationRequests.remove(transactionHandler);
			votingQuorums.remove(transactionHandler);
			coordinatorGroups.remove(transactionHandler);
		}

	}

	private class AtomicMulticastTask implements Callable<TerminationResult> {

		private ExecutionHistory executionHistory;

		private AtomicMulticastTask(ExecutionHistory eh) {
			this.executionHistory = eh;
		}

		public TerminationResult call() throws Exception {
			HashSet<String> destGroups = new HashSet<String>();
			Set<String> concernedKeys = ConsistencyFactory
					.getConcerningKeys(executionHistory);

			/*
			 * If there is no concerning key, it means that the transaction can
			 * commit right away. e.g. read-only transaction with NMSI
			 * consistency.
			 */
			if (concernedKeys.size() == 0) {
				executionHistory.changeState(TransactionState.COMMITTED);
				return new TerminationResult(
						executionHistory.getTransactionHandler(),
						TransactionState.COMMITTED, null);
			}

			destGroups.addAll(jessy.partitioner.resolveNames(concernedKeys));

			/*
			 * if this process will receive this transaction through atomic
			 * multicast then certifyAtCoordinator is set to true, otherwise, it
			 * is set to false.
			 */
			executionHistory.setCoordinator(membership.myId());
			if (destGroups.contains(group.name())) {
				executionHistory.setCertifyAtCoordinator(true);
			} else {
				executionHistory.setCertifyAtCoordinator(false);
			}

			/*
			 * Atomic multicast the transaction.
			 */
			atomicMulticastStream
					.atomicMulticast(new TerminateTransactionRequestMessage(
							executionHistory, destGroups, group.name(),
							membership.myId()));

			/*
			 * Wait here until the result of the transaction is known. While is
			 * for preventing <i>spurious wakeup</i>
			 */
			synchronized (executionHistory.getTransactionHandler()) {
				while (!terminationResults.containsKey(executionHistory
						.getTransactionHandler().getId())) {
					executionHistory.getTransactionHandler().wait();
				}
			}

			TerminationResult result = terminationResults.get(executionHistory
					.getTransactionHandler().getId());
			/*
			 * garbage collect he result. We do not need it anymore. Note, you
			 * cannot garbage collect {@code terminationResults} in {@code
			 * garbageCollect}.
			 */
			terminationResults.remove(executionHistory.getTransactionHandler()
					.getId());
			return result;
		}
	}

	private class CertifyAndVoteTask implements Callable<Boolean> {

		private TerminateTransactionRequestMessage msg;

		private CertifyAndVoteTask(TerminateTransactionRequestMessage msg) {
			this.msg = msg;
		}

		public Boolean call() throws Exception {

			try {

				/*
				 * First, it needs to run the certification test on the received
				 * execution history. A blind write always succeeds.
				 */
				boolean certified = msg.getExecutionHistory()
						.getTransactionType() == BLIND_WRITE
						|| ConsistencyFactory.getConsistency().certify(
								jessy.getLastCommittedEntities(),
								msg.getExecutionHistory());

				jessy.setExecutionHistory(msg.getExecutionHistory());

				/*
				 * If it holds all keys, it can decide <i>locally</i> according
				 * to the value of <@code certified>.
				 * 
				 * Otherwise, it should vote and wait to get a voting quorums;
				 */
				if (msg.gDest.size() == 1 && msg.gDest.contains(group.name())) {
					msg.getExecutionHistory()
							.changeState(
									(certified) ? TransactionState.COMMITTED
											: TransactionState.ABORTED_BY_CERTIFICATION);
				} else {

					/*
					 * Compute a set of destinations for the votes, and sends
					 * out the votes to all replicas <i>that replicate objects
					 * modified inside the transaction</i>.
					 */
					Set<String> dest = jessy.partitioner.resolveNames(msg
							.getExecutionHistory().getWriteSet().getKeys());
					
					Vote vote= new Vote(msg
							.getExecutionHistory().getTransactionHandler(),
							certified, group.name(), msg.gDest);
					voteStream.multicast(new VoteMessage(vote, dest, group
							.name(), membership.myId()));
					addVote(vote);

					logger.debug("voting quorum for "+msg.getExecutionHistory().getTransactionHandler());
					logger.debug("result is "+ votingQuorums.get(
								msg.getExecutionHistory().getTransactionHandler())
								.getTerminationResult());
					
					TransactionState state = votingQuorums.get(
							msg.getExecutionHistory().getTransactionHandler())
							.getTerminationResult();
					msg.getExecutionHistory().changeState(state);
				}

				/*
				 * The transaction is decided. creates a termination result, and
				 * passes it to the {@code handleTerminationResult}, and returns
				 * true.
				 */
				TerminationResult terminationResult = new TerminationResult(msg
						.getExecutionHistory().getTransactionHandler(), msg
						.getExecutionHistory().getTransactionState(), !msg
						.getExecutionHistory().isCertifyAtCoordinator());
				handleTerminationResult(msg.getExecutionHistory(),
						terminationResult);

			} catch (Exception e) {
				e.printStackTrace();
			}
			return true;

		}
	}
}
