package fr.inria.jessy.transaction.termination;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.apache.log4j.Logger;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.TerminationCommunication;
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
public class DistributedTermination implements Learner {

	private static Logger logger = Logger
			.getLogger(DistributedTermination.class);
	private static ValueRecorder concurrentCollectionsSize;

	private DistributedJessy jessy;

	private ExecutorPool pool = ExecutorPool.getInstance();

	private TerminationCommunication terminationCommunication;

	private Group group;

	private Map<UUID, TransactionHandler> terminationRequests;

	/**
	 * VotingQuorums for processing transactions.
	 */
	private ConcurrentHashMap<TransactionHandler, VotingQuorum> votingQuorums;

	/**
	 * Atomically delivered but not processed transactions
	 */
	private LinkedList<TerminateTransactionRequestMessage> atomicDeliveredMessages;

	/**
	 * Terminated transactions
	 */
	private Map<TransactionHandler, Integer> terminated;

	static {
		concurrentCollectionsSize = new ValueRecorder(
				"DistributedTermination#concurrentMapSize");
		concurrentCollectionsSize.setFormat("%M");
	}

	public DistributedTermination(DistributedJessy j) {
		jessy = j;
		group = JessyGroupManager.getInstance().getMyGroup();
		terminationCommunication = jessy.getConsistency()
				.getOrCreateTerminationCommunication(group, this);
		logger.info("initialized");

		terminationRequests = new ConcurrentHashMap<UUID, TransactionHandler>();
		atomicDeliveredMessages = new LinkedList<TerminateTransactionRequestMessage>();
		votingQuorums = new ConcurrentHashMap<TransactionHandler, VotingQuorum>();

		terminated = new ConcurrentLinkedHashMap.Builder<TransactionHandler, Integer>()
				.maximumWeightedCapacity(1000) // FIXME
				.build();
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

			synchronized (atomicDeliveredMessages) {
				atomicDeliveredMessages.offer(terminateRequestMessage);
			}

			pool.submit(new CertifyAndVoteTask(terminateRequestMessage));

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
	 * Upon receiving a new certification vote, it is added to the
	 * votingQuorums.
	 * 
	 * @param vote
	 */
	private void addVote(Vote vote) {
		VotingQuorum vq = votingQuorums.putIfAbsent(
				vote.getTransactionHandler(),
				new VotingQuorum(vote.getTransactionHandler()));
		if (vq == null) {
			logger.debug("creating voting quorum for "
					+ vote.getTransactionHandler());
			vq = votingQuorums.get(vote.getTransactionHandler());
		}

		try {
			vq.addVote(vote);

		} catch (Exception ex) {
			/*
			 * If here is reached, it means that a concurrent thread has already
			 * garbage collected the vq. Thus it has become null. <p> No special
			 * task is needed to be performed.
			 */
		}
	}

	/**
	 * This method is called one a replica has received the votes. If the
	 * transaction is able to commit, first it is prepared through
	 * {@code Consistency#prepareToCommit(ExecutionHistory)} then its modified
	 * entities are applied.
	 */
	private void handleTerminationResult(ExecutionHistory executionHistory)
			throws Exception {

		TransactionHandler th = executionHistory.getTransactionHandler();
		logger.debug("handling termination result of " + th);
		assert !terminated.containsKey(th);

		if (executionHistory.getTransactionState() == TransactionState.COMMITTED) {
			logger.debug("Applying modified entities of committed transaction "
					+ th.getId());

			/*
			 * Prepare the transaction. I.e., update the vectors of modified
			 * entities.
			 */
			jessy.getConsistency().prepareToCommit(executionHistory);

			/*
			 * Apply the modified entities.
			 */
			jessy.applyModifiedEntities(executionHistory);

			/*
			 * calls the postCommit method of the consistency criterion for post
			 * commit actions. (e.g., propagating vectors)
			 */
			jessy.getConsistency().postCommit(executionHistory);
		}

		jessy.garbageCollectTransaction(executionHistory
				.getTransactionHandler());

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

		terminationRequests.remove(transactionHandler);
		votingQuorums.remove(transactionHandler);

		terminated.put(transactionHandler, 0);

		concurrentCollectionsSize.add(terminationRequests.size()
				+ votingQuorums.size() + atomicDeliveredMessages.size()
				+ terminated.size());

	}

	private class AtomicMulticastTask implements Callable<TransactionState> {

		private ExecutionHistory executionHistory;

		private AtomicMulticastTask(ExecutionHistory eh) {
			this.executionHistory = eh;
		}

		public TransactionState call() throws Exception {
			HashSet<String> destGroups = new HashSet<String>();
			Set<String> concernedKeys = jessy.getConsistency()
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

			executionHistory.setCoordinator(JessyGroupManager.getInstance().getSourceId());

			destGroups.addAll(jessy.partitioner.resolveNames(concernedKeys));
			if (destGroups.contains(group.name())) {
				executionHistory.setCertifyAtCoordinator(true);
			} else {
				executionHistory.setCertifyAtCoordinator(false);
			}

			votingQuorums.put(executionHistory.getTransactionHandler(),
					new VotingQuorum(executionHistory.getTransactionHandler()));

			/*
			 * gets the pointer for the transaction's VotingQuorum because the
			 * votingQuorums might be garbage collected by another thread after
			 * multicasting this transaction.
			 */
			VotingQuorum vq = votingQuorums.get(executionHistory
					.getTransactionHandler());

			logger.debug("A node in Group " + group
					+ " send a termination message "
					+ executionHistory.getTransactionHandler().getId() + " to "
					+ destGroups);
			/*
			 * Atomic multicast the transaction.
			 */
			terminationCommunication.sendTerminateTransactionRequestMessage(
					new TerminateTransactionRequestMessage(executionHistory,
							destGroups, group.name(), JessyGroupManager.getInstance().getSourceId()),
					destGroups);

			/*
			 * Wait here until the result of the transaction is known.
			 */
			TransactionState result = vq.waitVoteResult(destGroups);

			if (!executionHistory.isCertifyAtCoordinator())
				garbageCollect(executionHistory.getTransactionHandler());

			return result;
		}
	}

	private class CertifyAndVoteTask implements Runnable {

		private TerminateTransactionRequestMessage msg;

		private CertifyAndVoteTask(TerminateTransactionRequestMessage m) {
			msg = m;
		}

		public void run() {

			try {

				/*
				 * First, à la P-Store.
				 */
				synchronized (atomicDeliveredMessages) {
					while (true) {
						boolean isConflicting = true;
						for (TerminateTransactionRequestMessage n : atomicDeliveredMessages) {
							if (n.equals(msg)) {
								isConflicting = false;
								break;
							}
							if (jessy.getConsistency().certificationCommute(
									n.getExecutionHistory(),
									msg.getExecutionHistory()))
								break;
						}
						if (isConflicting)
							atomicDeliveredMessages.wait();
						else
							break;
					}
				}

				/*
				 * Second, it needs to run the certification test on the
				 * received execution history. A blind write always succeeds.
				 */
				boolean isAborted = msg.getExecutionHistory()
						.getTransactionType() == BLIND_WRITE
						|| jessy.getConsistency().certify(
								msg.getExecutionHistory());

				jessy.setExecutionHistory(msg.getExecutionHistory());

				/*
				 * Computes a set of destinations for the votes, and sends out
				 * the votes to all replicas <i>that replicate objects modified
				 * inside the transaction</i>. The group this node belongs to is
				 * ommitted.
				 */

				Vote vote = new Vote(msg.getExecutionHistory()
						.getTransactionHandler(), isAborted, group.name());

				/*
				 * The votes will be sent to all concerned keys. Note that the
				 * optimization to only send the votes to the nodes replicating
				 * objects in the writeset is not included. Thus, for example,
				 * under serializability, a node may wait to receive the votes
				 * from all nodes replicating the concerned keys, and then
				 * returns without performing anything.
				 */
				Set<String> dest = new HashSet<String>();
				dest.addAll(jessy.partitioner.resolveNames(jessy
						.getConsistency().getConcerningKeys(
								msg.getExecutionHistory())));

				dest.remove(group.name());
				VoteMessage voteMsg = new VoteMessage(vote, dest, group.name(),
						JessyGroupManager.getInstance().getSourceId());

				terminationCommunication.sendVote(voteMsg, msg
						.getExecutionHistory().isCertifyAtCoordinator(), msg
						.getExecutionHistory().getCoordinator());

				addVote(vote);

				TransactionState state = votingQuorums.get(
						msg.getExecutionHistory().getTransactionHandler())
						.waitVoteResult(dest);

				logger.debug("got voting quorum for "
						+ msg.getExecutionHistory().getTransactionHandler()
						+ " , result is " + state);

				msg.getExecutionHistory().changeState(state);

				handleTerminationResult(msg.getExecutionHistory());
				garbageCollect(msg.getExecutionHistory()
						.getTransactionHandler());

				synchronized (atomicDeliveredMessages) {
					atomicDeliveredMessages.remove(msg);
					atomicDeliveredMessages.notifyAll();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
}
