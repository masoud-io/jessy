package fr.inria.jessy.transaction.termination;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;
import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.READONLY_TRANSACTION;
import static fr.inria.jessy.transaction.TransactionState.COMMITTED;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.yahoo.ycsb.JessyDBClient;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.*;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.Partitioner;
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

	private DistributedJessy jessy;

	private ExecutorPool pool = ExecutorPool.getInstance();

	private WanAMCastStream amStream;
	private MulticastStream rmStream;
	private Membership membership;
	private Group group;

	private Map<UUID, TransactionHandler> terminationRequests;
	private Map<TransactionHandler, TerminationResult> terminationResults;
	private Map<TransactionHandler, VotingQuorum> votingQuorums;
	private Map<TransactionHandler, String> coordinatorGroups;

	private Queue<TerminateTransactionRequestMessage> atomicDeliveredMessages;
	private Map<TransactionHandler, TerminateTransactionRequestMessage> processingMessages;

	private List<TransactionHandler> terminated;
	private Thread thread;

	public DistributedTermination(DistributedJessy j, Group g) {

		jessy = j;
		membership = j.membership;
		group = g;

		rmStream = FractalManager.getInstance().getOrCreateMulticastStream(
				"Jessy", group.name());
		rmStream.registerLearner("VoteMessage", this);
		rmStream.registerLearner("TerminateTransactionReplyMessage", this);

		amStream = FractalManager.getInstance().getOrCreateWanAMCastStream(
				"Jessy", group.name());
		amStream.registerLearner("TerminateTransactionRequestMessage", this);

		rmStream.start();
		amStream.start();

		terminationRequests = new ConcurrentHashMap<UUID, TransactionHandler>();
		terminationResults = new ConcurrentHashMap<TransactionHandler, TerminationResult>();
		atomicDeliveredMessages = new LinkedBlockingQueue<TerminateTransactionRequestMessage>();
		processingMessages = new ConcurrentHashMap<TransactionHandler, TerminateTransactionRequestMessage>();
		votingQuorums = new ConcurrentHashMap<TransactionHandler, VotingQuorum>();
		coordinatorGroups = new ConcurrentHashMap<TransactionHandler, String>();

		terminated = new ArrayList<TransactionHandler>();

		thread = new Thread(this);
		thread.start();
	}

	public Future<TerminationResult> terminateTransaction(ExecutionHistory ex) {

		ex.changeState(TransactionState.COMMITTING);
		Future<TerminationResult> reply = pool.submit(new AtomicMulticastTask(
				ex));
		terminationRequests.put(ex.getTransactionHandler().getId(),
				ex.getTransactionHandler());
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

			assert terminationRequests.containsKey(replyMessage
					.getTerminationResult().getTransactionHandler().getId())
					|| terminated.contains(replyMessage.getTerminationResult()
							.getTransactionHandler());

			TransactionHandler th = replyMessage.getTerminationResult()
					.getTransactionHandler();

			if (terminated.contains(th))
				return;

			// We call with null, cause clearly we are not in the WReplicaSet.
			handleTerminationResult(null, replyMessage.getTerminationResult());

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
	 */
	@Override
	public void run() {
		TerminateTransactionRequestMessage msg;

		while (true) {

			msg = atomicDeliveredMessages.peek();
			if (msg == null)
				continue;

			/*
			 * if there is a conflict, it cannot proceed to handle the head
			 * message, thus waits until one of a messages in {@link
			 * 
			 * @processingMessgaes} finishes and notify this queue.
			 */
			for (TerminateTransactionRequestMessage committing : processingMessages
					.values()) {
				if (ConsistencyFactory.getConsistency().hasConflict(
						msg.getExecutionHistory(),
						committing.getExecutionHistory())) {
					try {
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

			/*
			 * There is no conflict with already processing messages in {@code
			 * processingMessages}, thus a new {@code CertifyAndVoteTask} is
			 * created for handling this messages.
			 */
			msg = atomicDeliveredMessages.poll();
			processingMessages.put(msg.getExecutionHistory()
					.getTransactionHandler(), msg);
			pool.submit(new CertifyAndVoteTask(msg));

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
	 * TODO should it be synchronized?
	 */
	private synchronized void handleTerminationResult(
			ExecutionHistory executionHistory,
			TerminationResult terminationResult) {

		TransactionHandler transactionHandler = terminationRequests
				.get(terminationResult.getTransactionHandler().getId());

		HashSet<String> cordinatorGroup = new HashSet<String>();
		cordinatorGroup.add(coordinatorGroups.get(terminationResult
				.getTransactionHandler()));
		assert executionHistory != null || transactionHandler != null;

		/*
		 * Apply the transaction if (i) it is committed and (ii) we hold a
		 * modified entity. Observe that if executionHistory==null, then we are
		 * the coord. and we own no modified entity.
		 */
		if (terminationResult.getTransactionState() == COMMITTED
				&& executionHistory != null) {
			boolean hasLocal = false;
			for (JessyEntity e : executionHistory.getWriteSet().getEntities()) {
				if (jessy.partitioner.isLocal(e.getSecondaryKey())) {
					try {
						hasLocal = true;
						jessy.performNonTransactionalLocalWrite(e);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
			}
			for (JessyEntity e : executionHistory.getCreateSet().getEntities()) {
				if (jessy.partitioner.isLocal(e.getSecondaryKey())) {
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
		 * decided the outcome of the transaction. So, if the coordinator will
		 * not received the outcome, this process will reliable multicast the
		 * result to the coordinator group.
		 */
		if (transactionHandler != null) {
			terminationResults.put(terminationResult.getTransactionHandler(),
					terminationResult);
			synchronized (transactionHandler) {
//				System.out.println("Before Notify: " + terminationResult);
				transactionHandler.notify();
			}
		} else if (terminationResult.isSendBackToCoordinator()) {
			MulticastMessage replyMessage = new TerminateTransactionReplyMessage(
					terminationResult, cordinatorGroup, group.name(),
					membership.myId());
			rmStream.multicast(replyMessage);
		}

		garbageCollect(terminationResult.getTransactionHandler());
	}

	/**
	 * Garbage collect all concurrent hash maps entries for the given
	 * {@code transactionHandler} TODO is it necessary to be synchronized?
	 * 
	 * @param transactionHandler
	 *            The transactionHandler to be garbage collected.
	 */
	private synchronized void garbageCollect(
			TransactionHandler transactionHandler) {
		terminated.add(transactionHandler);

		if (terminationRequests.containsKey(transactionHandler.getId())) {
			terminationRequests.remove(transactionHandler);
			terminationResults.remove(transactionHandler);
			votingQuorums.remove(transactionHandler);
			coordinatorGroups.remove(transactionHandler);
		}

		processingMessages.remove(transactionHandler);

		// PIERRE ??
		// synchronized(thread){
		// thread.notify();
		// }

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

			destGroups.addAll(jessy.partitioner
					.resolveToGroupNames(concernedKeys));

			/*
			 * if this process will receive this transaction through atomic
			 * multicast then certifyAtCoordinator is set to true, otherwise, it
			 * is set to false.
			 */
			if (destGroups.contains(group.name())) {
				executionHistory.setCertifyAtCoordinator(true);
			} else {
				executionHistory.setCertifyAtCoordinator(false);
			}

			/*
			 * Atomic multicast the transaction.
			 */
			amStream.atomicMulticast(new TerminateTransactionRequestMessage(
					executionHistory, destGroups, group.name(), membership
							.myId()));

			synchronized (executionHistory.getTransactionHandler()) {
				executionHistory.getTransactionHandler().wait();
			}

//			System.out.println("Before Result: " + terminationResults.values());
			TerminationResult result = terminationResults.get(executionHistory
					.getTransactionHandler());
			return result;
		}
	}

	private class CertifyAndVoteTask implements Callable<Boolean> {

		private TerminateTransactionRequestMessage msg;

		private CertifyAndVoteTask(TerminateTransactionRequestMessage msg) {
			this.msg = msg;
		}

		public Boolean call() throws Exception {

			/*
			 * First, it needs to run the certification test on the received
			 * execution history. A blind write always succeeds.
			 */
			boolean certified = msg.getExecutionHistory().getTransactionType() == BLIND_WRITE
					|| ConsistencyFactory.getConsistency().certify(
							jessy.getLastCommittedEntities(),
							msg.getExecutionHistory());

			/*
			 * Compute a set of destinations for the votes, and sends out the
			 * votes to all replicas <i>that replicate objects modified inside
			 * the transaction</i>.
			 */
			Set<String> dest = jessy.partitioner.resolveToGroupNames(msg
					.getExecutionHistory().getWriteSet().getKeys());
			rmStream.multicast(new VoteMessage(new Vote(msg
					.getExecutionHistory().getTransactionHandler(), certified,
					group.name(), msg.gDest), dest, group.name(), membership
					.myId()));

			/*
			 * If the transaction is read-only, and the processing thread is not
			 * the coordinator, it has nothing else to do. It can returns
			 * without knowing the <i>outcome</i> .
			 * 
			 * Those processes that receive votes, will decide the outcome of
			 * the transaction and send the results to the coordinator.
			 * 
			 * <p> It just garbage collect, and returns true.
			 */
			if (msg.getExecutionHistory().getTransactionType() == READONLY_TRANSACTION
					&& !msg.gSource.equals(group.name())) {
				garbageCollect(msg.getExecutionHistory()
						.getTransactionHandler());
				return true;
			}

			/*
			 * if it holds all keys, it can also decide <i>locally</i> according
			 * to the value of <@code certified>.
			 * 
			 * Otherwise, it should wait to get a voting quorums;
			 */
			if (msg.gDest.size() == 1 && msg.gDest.contains(group.name())) {
				msg.getExecutionHistory().changeState(
						(certified) ? TransactionState.COMMITTED
								: TransactionState.ABORTED_BY_CERTIFICATION);
			} else {
				TransactionState state = votingQuorums.get(
						msg.getExecutionHistory().getTransactionHandler())
						.getTerminationResult();
				msg.getExecutionHistory().changeState(state);
			}

			/*
			 * creates a termination result, and passes it to the {@code
			 * handleTerminationResult}, and returns true.
			 */
			TerminationResult terminationResult = new TerminationResult(msg
					.getExecutionHistory().getTransactionHandler(), msg
					.getExecutionHistory().getTransactionState(), !msg
					.getExecutionHistory().isCertifyAtCoordinator());
			handleTerminationResult(msg.getExecutionHistory(),
					terminationResult);
			return true;
		}
	}
}
