package fr.inria.jessy.transaction.termination;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;
import static fr.inria.jessy.transaction.TransactionState.COMMITTED;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;

import org.apache.log4j.Logger;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.termination.message.VoteMessage;

/**
 * This class is responsible for sending transactions to remote replicas,
 * receiving the certification votes from remote replicas, deciding the outcome
 * of the transaction, and finally applying the transaction changes to the local
 * store.
 * 
 * @author Masoud Saeida Ardeknai
 * 
 */
public class DistributedTermination implements Learner, Runnable {

	private static Logger logger = Logger
			.getLogger(DistributedTermination.class);
	private static ValueRecorder concurrentCollectionsSize;

	private DistributedJessy jessy;

	private ExecutorPool pool = ExecutorPool.getInstance();

	private WanAMCastStream atomicMulticastStream;
	private MulticastStream voteStream;
	private MulticastStream terminationNotificationStream;

	private Membership membership;
	private Group group;

	private Map<UUID, TransactionHandler> terminationRequests;

	/**
	 * VotingQuorums for processing transactions.
	 */
	private ConcurrentHashMap<TransactionHandler, VotingQuorum> votingQuorums;

	/**
	 * Atomically delivered but not processed transactions
	 */
	private LinkedBlockingQueue<TerminateTransactionRequestMessage> atomicDeliveredMessages;

	/**
	 * Atomically delivered and processing transactions
	 */
	private Map<TransactionHandler, TerminateTransactionRequestMessage> processingMessages;

	/**
	 * Terminated transactions
	 */
	private Map<TransactionHandler, Integer> terminated;

	/**
	 * Local thread for executing waiting transactions in
	 * {@link atomicDeliveredMessages}
	 */
	private Thread thread;

	static {
		concurrentCollectionsSize = new ValueRecorder(
				"DistributedTermination#concurrentMapSize");
		concurrentCollectionsSize.setFormat("%M");
	}

	public DistributedTermination(DistributedJessy j, Group g) {

		jessy = j;
		membership = j.membership;
		group = g;

		terminationNotificationStream = FractalManager.getInstance()
				.getOrCreateMulticastStream(ConstantPool.JESSY_ALL_GROUP,
						ConstantPool.JESSY_ALL_GROUP);
		terminationNotificationStream.registerLearner("VoteMessage", this);

		voteStream = FractalManager.getInstance().getOrCreateMulticastStream(
				group.name(), group.name());
		voteStream.registerLearner("VoteMessage", this);

		atomicMulticastStream = FractalManager.getInstance()
				.getOrCreateWanAMCastStream(group.name(), group.name());
		voteStream.registerLearner("TerminateTransactionRequestMessage", this);

		terminationNotificationStream.start();
		voteStream.start();
		atomicMulticastStream.start();

		terminationRequests = new ConcurrentHashMap<UUID, TransactionHandler>();
		atomicDeliveredMessages = new LinkedBlockingQueue<TerminateTransactionRequestMessage>();
		processingMessages = new ConcurrentHashMap<TransactionHandler, TerminateTransactionRequestMessage>();
		votingQuorums = new ConcurrentHashMap<TransactionHandler, VotingQuorum>();

		terminated = new ConcurrentLinkedHashMap.Builder<TransactionHandler, Integer>()
				.maximumWeightedCapacity(1000) // FIXME
				.build();

		thread = new Thread(this);
		thread.start();
	}

	/**
	 * Called by distributed jessy for submitting a new transaction for
	 * termination.
	 * 
	 * @param ex
	 *            ExecutionHistory of the transaction for termination.
	 * @return
	 */
	public Future<TransactionState> terminateTransaction(ExecutionHistory ex) {

		logger.debug("terminate transaction "
				+ ex.getTransactionHandler().getId());
		ex.changeState(TransactionState.COMMITTING);
		terminationRequests.put(ex.getTransactionHandler().getId(),
				ex.getTransactionHandler());
		Future<TransactionState> reply = pool
				.submit(new AtomicMulticastTask(ex));
		return reply;
	}

	/**
	 * Call back by Fractal upon receiving atomically delivered
	 * {@link TerminateTransactionRequestMessage} or {@link Vote}.
	 */
	@Deprecated
	public void learn(Stream s, Serializable v) {

		if (v instanceof TerminateTransactionRequestMessage) {

			TerminateTransactionRequestMessage terminateRequestMessage = (TerminateTransactionRequestMessage) v;

			logger.debug("got a TerminateTransactionRequestMessage for "
					+ terminateRequestMessage.getExecutionHistory()
							.getTransactionHandler().getId());

			atomicDeliveredMessages.add(terminateRequestMessage);

		} else { // VoteMessage

			Vote vote = ((VoteMessage) v).getVote();

			logger.debug("got a VoteMessage from " + vote.getVoterGroupName()
					+ " for " + vote.getTransactionHandler().getId());

			if (terminated.containsKey(vote.getTransactionHandler()))
				return;
			addVote(vote);

		}

	}

	/**
	 * Continuously, checks {@code atomicDeliveredMessages} queue, and if the
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

				logger.debug("atomic deliver a TerminateTransactionRequestMessage for "
						+ terminateRequestMessage.getExecutionHistory()
								.getTransactionHandler().getId());

				Iterator<TerminateTransactionRequestMessage> itr = processingMessages
						.values().iterator();
				while (itr.hasNext()) {
					processingMessage = itr.next();

					// FIXME this is false....
					// if (ConsistencyFactory.getConsistency().hasConflict(
					// terminateRequestMessage.getExecutionHistory(),
					// processingMessage.getExecutionHistory())) {
					// synchronized (processingMessage) {
					// logger.debug("waiting termination of "
					// +
					// processingMessage.getExecutionHistory().getTransactionHandler().getId());
					// processingMessage.wait();
					// logger.debug("having termination of "
					// +
					// processingMessage.getExecutionHistory().getTransactionHandler().getId());
					// }
					// itr = processingMessages.values().iterator();
					// continue;
					// }
				}
				processingMessages.put(terminateRequestMessage
						.getExecutionHistory().getTransactionHandler(),
						terminateRequestMessage);

				/*
				 * There is no conflict with already processing messages in
				 * {@code processingMessages}, thus a new {@code
				 * CertifyAndVoteTask} is created for handling this messages.
				 */
				logger.debug("create a CertifyAndVoteTask for "
						+ terminateRequestMessage.getExecutionHistory()
								.getTransactionHandler().getId());

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
		VotingQuorum vq = votingQuorums.putIfAbsent(
				vote.getTransactionHandler(),
				new VotingQuorum(vote.getTransactionHandler(), vote
						.getAllVoterGroups()));
		if (vq == null) {
			logger.debug("creating voting quorum for "
					+ vote.getTransactionHandler());
			vq = votingQuorums.get(vote.getTransactionHandler());
		}
		vq.addVote(vote);
	}

	/**
	 * This mehod is called one a replica has received the votes. If the
	 * transaction is able to commit, first it is prepared through
	 * {@code Consistency#prepareToCommit(ExecutionHistory)} then its modified
	 * entities are applied.
	 */
	private void handleTerminationResult(ExecutionHistory executionHistory)
			throws Exception {

		TransactionHandler th = executionHistory.getTransactionHandler();
		logger.debug("handling termination result of " + th);
		assert !terminated.containsKey(th);

		if (executionHistory.getTransactionState() == COMMITTED) {
			logger.debug("Applying modified entities of committed transaction "
					+ th.getId());
			jessy.getConsistency().prepareToCommit(executionHistory);
			jessy.applyModifiedEntities(executionHistory);
		}

	}

	/**
	 * Garbage collect all concurrent hash maps entries for the given
	 * {@code transactionHandler}
	 * 
	 * 
	 * @param transactionHandler
	 *            The transactionHandler to be garbage collected.
	 */
	private void garbageCollect(TransactionHandler transactionHandler) {

		logger.debug("garbage-collect for " + transactionHandler.getId());

		processingMessages.remove(transactionHandler);
		terminationRequests.remove(transactionHandler);
		votingQuorums.remove(transactionHandler);

		terminated.put(transactionHandler, 0);

		concurrentCollectionsSize.add(terminationRequests.size()
				+ votingQuorums.size() + atomicDeliveredMessages.size()
				+ processingMessages.size() + terminated.size());

	}

	private class AtomicMulticastTask implements Callable<TransactionState> {

		private ExecutionHistory executionHistory;

		private AtomicMulticastTask(ExecutionHistory eh) {
			this.executionHistory = eh;
		}

		public TransactionState call() throws Exception {
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
				return TransactionState.COMMITTED;
			}

			executionHistory.setCoordinator(membership.myId());
			if (destGroups.contains(group.name())) {
				executionHistory.setCertifyAtCoordinator(true);
			} else {
				executionHistory.setCertifyAtCoordinator(false);
			}

			destGroups.addAll(jessy.partitioner.resolveNames(concernedKeys));
			votingQuorums.put(executionHistory.getTransactionHandler(),
					new VotingQuorum(executionHistory.getTransactionHandler(),
							destGroups));

			/*
			 * gets the pointer for the transaction's VotingQuorum because the
			 * votingQuorums might be garbage collected by another thread after
			 * multicasting this transaction.
			 */
			VotingQuorum vq = votingQuorums.get(executionHistory
					.getTransactionHandler());

			/*
			 * Atomic multicast the transaction.
			 */
			voteStream.multicast(new TerminateTransactionRequestMessage(
					executionHistory, destGroups, group.name(), membership
							.myId()));

			/*
			 * Wait here until the result of the transaction is known.
			 * 
			 * 
			 * TODO While is for preventing <i>spurious wakeup</i>
			 */

			return vq.waitVoteResult();

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
				boolean isAborted = msg.getExecutionHistory()
						.getTransactionType() == BLIND_WRITE
						|| jessy.getConsistency().certify(
								msg.getExecutionHistory());

				jessy.setExecutionHistory(msg.getExecutionHistory());

				/*
				 * Compute a set of destinations for the votes, and sends out
				 * the votes to all replicas <i>that replicate objects modified
				 * inside the transaction</i>. The group this node belongs to is
				 * ommitted.
				 */

				Vote vote = new Vote(msg.getExecutionHistory()
						.getTransactionHandler(), isAborted, group.name(),
						msg.dest);

				Set<String> dest = jessy.partitioner.resolveNames(msg
						.getExecutionHistory().getWriteSet().getKeys());
				dest.remove(group.name());
				VoteMessage voteMsg = new VoteMessage(vote, dest, group.name(),
						membership.myId());

				voteStream.multicast(voteMsg);

				if (!msg.getExecutionHistory().isCertifyAtCoordinator())
					terminationNotificationStream.unicast(voteMsg, msg
							.getExecutionHistory().getCoordinator());

				addVote(vote);

				TransactionState state = votingQuorums.get(
						msg.getExecutionHistory().getTransactionHandler())
						.waitVoteResult();

				logger.debug("got voting quorum for "
						+ msg.getExecutionHistory().getTransactionHandler()
						+ " , result is " + state);

				msg.getExecutionHistory().changeState(state);
				handleTerminationResult(msg.getExecutionHistory());
				garbageCollect(msg.getExecutionHistory()
						.getTransactionHandler());

			} catch (Exception e) {
				e.printStackTrace();
			}
			return true;

		}
	}
}
