package fr.inria.jessy.transaction.termination;

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
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.consistency.Consistency.ConcernedKeysTarget;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;

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

	private static ValueRecorder certificationTime;

	private static ValueRecorder transactionTerminationTime;

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
				"DistributedTermination#concurrentCollectionSize");
		concurrentCollectionsSize.setFormat("%M");
		
		certificationTime = new ValueRecorder("Jessy#certificationTime(ms)");
		certificationTime.setFormat("%a");
		certificationTime.setFactor(1000000);

		transactionTerminationTime = new ValueRecorder(
				"Jessy#transactionTerminationTime(ms)");
		transactionTerminationTime.setFormat("%a");
		transactionTerminationTime.setFactor(1000000);

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
				.maximumWeightedCapacity(1000) // FIXME works ???
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

			terminateRequestMessage.getExecutionHistory()
					.setStartCertification(System.nanoTime());
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
			jessy.getConsistency().voteReceived(vote);
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
	private void handleTerminationResult(TerminateTransactionRequestMessage msg)
			throws Exception {

		ExecutionHistory executionHistory = msg.getExecutionHistory();

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

		}

		synchronized (atomicDeliveredMessages) {
			atomicDeliveredMessages.remove(msg);
			atomicDeliveredMessages.notifyAll();
		}

		if (executionHistory.getTransactionState() == TransactionState.COMMITTED) {
			logger.debug("Applying modified entities of committed transaction "
					+ th.getId());
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

			long start = System.nanoTime();
			TransactionState result;

			HashSet<String> destGroups = new HashSet<String>();
			Set<String> concernedKeys = jessy.getConsistency()
					.getConcerningKeys(executionHistory,
							ConcernedKeysTarget.ATOMIC_MULTICAST);

			/*
			 * If there is no concerning key, it means that the transaction can
			 * commit right away. e.g. read-only transaction with NMSI
			 * consistency.
			 */
			if (concernedKeys.size() == 0) {
				
				transactionTerminationTime.add(System.nanoTime() - start);
				executionHistory.changeState(TransactionState.COMMITTED);
				result = TransactionState.COMMITTED;
				
			}else{

				executionHistory.setCoordinator(JessyGroupManager.getInstance()
						.getSourceId());

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
				executionHistory.clearReadValues();
				terminationCommunication.sendTerminateTransactionRequestMessage(
						new TerminateTransactionRequestMessage(executionHistory,
								destGroups, group.name(), JessyGroupManager
								.getInstance().getSourceId()), destGroups);

				/*
				 * Wait here until the result of the transaction is known.
				 */
				result = vq.waitVoteResult(destGroups);

				if (result == TransactionState.COMMITTED)
					transactionTerminationTime.add(System.nanoTime() - start);
				
			}

			if (!executionHistory.isCertifyAtCoordinator()){
				garbageCollect(executionHistory.getTransactionHandler());
			}

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

						boolean isConflicting = false;

						for (TerminateTransactionRequestMessage n : atomicDeliveredMessages) {
							if (n.equals(msg)) {
								break;
							}
							if (!jessy.getConsistency().certificationCommute(
									n.getExecutionHistory(),
									msg.getExecutionHistory())) {
								isConflicting = true;
								break;
							}
						}
						if (isConflicting)
							atomicDeliveredMessages.wait();
						else
							break;
					}
				}

				jessy.setExecutionHistory(msg.getExecutionHistory());

				Vote vote = jessy.getConsistency().createCertificationVote(
						msg.getExecutionHistory());

				/*
				 * Computes a set of destinations for the votes, and sends out
				 * the votes to all replicas <i>that replicate objects modified
				 * inside the transaction</i>. The group this node belongs to is
				 * ommitted.
				 * 
				 * <p>
				 * 
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
								msg.getExecutionHistory(),
								ConcernedKeysTarget.EXCHANGE_VOTES)));

				/*
				 * if true, it means that it must wait for the vote from the
				 * others and apply the changes, otherwise, it only needs to
				 * send its vote, and garbage collect. For example, in SER, an
				 * instance which only replicates an object read by the
				 * transaction should send its vote, and return.
				 */
				boolean replicateWrittenObjects = (dest.contains(group.name())) ? true
						: false;
				{

					dest.remove(group.name());
					VoteMessage voteMsg = new VoteMessage(vote, dest,
							group.name(), JessyGroupManager.getInstance()
									.getSourceId());

					terminationCommunication.sendVote(voteMsg, msg
							.getExecutionHistory().isCertifyAtCoordinator(),
							msg.getExecutionHistory().getCoordinator());

					addVote(vote);
				}

				if (replicateWrittenObjects) {
					
					TransactionState state = votingQuorums.get(
							msg.getExecutionHistory().getTransactionHandler())
							.waitVoteResult(dest);

					logger.debug("got voting quorum for "
							+ msg.getExecutionHistory().getTransactionHandler()
							+ " , result is " + state);
					
					if (state == TransactionState.COMMITTED)
						certificationTime.add(System.nanoTime()
								- msg.getExecutionHistory().getStartCertification());

					msg.getExecutionHistory().changeState(state);

				}

				handleTerminationResult(msg);
				garbageCollect(msg.getExecutionHistory()
						.getTransactionHandler());

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
}
