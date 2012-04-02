package fr.inria.jessy.transaction.termination;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.rmcast.RMCastStream;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import utils.ExecutorPool;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.Partitioner;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionReplyMessage;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.termination.message.VoteMessage;

//TODO COMMENT ME
//TODO Clean these ConcurrentMaps
public class DistributedTermination implements Learner, Runnable {

	private Jessy jessy;

	private ExecutorPool pool = ExecutorPool.getInstance();
	private WanAMCastStream amStream;
	private RMCastStream rmStream;

	private ConcurrentMap<TransactionHandler, Future<TerminationResult>> futures;
	private ConcurrentMap<TransactionHandler, TerminationResult> terminationResults;
	private ConcurrentMap<TransactionHandler, VotingQuorum> votingQuorums;
	private ConcurrentMap<TransactionHandler, String> coordinatorGroups;

	private BlockingQueue<TerminateTransactionRequestMessage> atomicDeliveredMessages;
	private ConcurrentMap<TransactionHandler, TerminateTransactionRequestMessage> processingMessages;

	private List<TransactionHandler> terminated;

	private String myGroup = Membership.getInstance().myGroup().name();

	Thread thread;

	public DistributedTermination(Jessy jessy) {
		this.jessy = jessy;

		Membership.getInstance().getOrCreateTCPGroup("ALLNODES");

		amStream = FractalManager.getInstance().getOrCreateWanAMCastStream(
				"DistributedTerminationStream",
				Membership.getInstance().myGroup().name());
		amStream.registerLearner("TerminateTransactionRequestMessage", this);
		amStream.registerLearner("TerminateTransactionReplyMessage", this);

		rmStream = FractalManager.getInstance().getOrCreateRMCastStream(
				"VoteMessage", Membership.getInstance().myGroup().name());
		rmStream.registerLearner("VoteMessage", this);

		futures = new ConcurrentHashMap<TransactionHandler, Future<TerminationResult>>();
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
		futures.put(ex.getTransactionHandler(), reply);
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

			if (terminated.contains(replyMessage.getTerminationResult()
					.getTransactionHandler())
					|| !futures.containsKey(replyMessage.getTerminationResult()
							.getTransactionHandler()))
				return;

			handleTerminationResult(replyMessage.getTerminationResult());
		} else {
			Vote vote = (Vote) v;
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
	 * called a remote replica. If the transaction is not going to be certified
	 * at the coordinator (coordinator does not replicate an object concerned by
	 * the transaction), the {@code terminationCode} should be send to the
	 * coordinator.
	 * <p>
	 * Instead of unicasting the result to only the coordinator, we atomic
	 * multicast the result to the group where the coodinator belongs to.
	 */
	private synchronized void handleTerminationResult(
			TerminationResult terminationResult) {

		Future<TerminationResult> future = futures.get(terminationResult
				.getTransactionHandler());
		HashSet<String> cordinatorGroup = new HashSet<String>();
		cordinatorGroup.add(coordinatorGroups.get(terminationResult
				.getTransactionHandler()));

		assert (terminationResult.isSendBackToCoordinator() && cordinatorGroup
				.contains(myGroup));

		/*
		 * if the future is not null, it means that this process is the
		 * coordinator, and it has received the outcome of a transaction
		 * termination phase. It puts the result in the terminationResults Map,
		 * and call the notify the future to finishes its task.
		 * 
		 * Otherwise, a non-cordinator has received the required votes, and has
		 * decided the outcome of the transaction. So, if the coordinator will
		 * not received the outcome, this process will atomic multicast the
		 * result to the coordinator group.
		 */
		if (future != null) {
			terminationResults.put(terminationResult.getTransactionHandler(),
					terminationResult);
			future.notify();
		} else if (terminationResult.isSendBackToCoordinator()
				&& !cordinatorGroup.contains(myGroup)) {
			TerminateTransactionReplyMessage replyMessage = new TerminateTransactionReplyMessage(
					terminationResult, cordinatorGroup, myGroup);
			amStream.atomicMulticast(replyMessage, cordinatorGroup);
		}

		garbageCollect(terminationResult.getTransactionHandler());
	}

	/**
	 * Garbage collect all concurrent hash maps entries for the given
	 * {@code transactionHandler}
	 * 
	 * @param transactionHandler
	 *            The transactionHandler to be garbage collected.
	 */
	private synchronized void garbageCollect(
			TransactionHandler transactionHandler) {
		terminated.add(transactionHandler);

		if (futures.containsKey(transactionHandler)) {
			futures.remove(transactionHandler);
			terminationResults.remove(transactionHandler);
			votingQuorums.remove(transactionHandler);
			coordinatorGroups.remove(transactionHandler);
		}

		processingMessages.remove(transactionHandler);
		thread.notify();

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
			 * If there is no concerningkey, it means that the transaction can
			 * commit right away. e.g. read-only transaction with NMSI
			 * consistency.
			 */
			if (concernedKeys.size() == 0) {
				executionHistory.changeState(TransactionState.COMMITTED);
				return new TerminationResult(
						executionHistory.getTransactionHandler(),
						TransactionState.COMMITTED, null);
			}

			destGroups.addAll(Partitioner.getInstance().resolveToGroupNames(
					concernedKeys));

			/*
			 * if this process will receive this transaction through atomic
			 * multicast, certifyAtCoordinator is set to true, otherwise, it is
			 * set to false.
			 */
			if (destGroups.contains(myGroup))
				executionHistory.setCertifyAtCoordinator(true);
			else
				executionHistory.setCertifyAtCoordinator(false);

			/*
			 * Atomic multicast the transaction.
			 */
			amStream.atomicMulticast(new TerminateTransactionRequestMessage(
					executionHistory, destGroups, myGroup), destGroups);
			futures.get(executionHistory.getTransactionHandler()).wait();
			return terminationResults.get(executionHistory
					.getTransactionHandler());
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
			 * execution history.
			 */
			boolean certified = ConsistencyFactory.getConsistency()
					.certify(jessy.getLastCommittedEntities(),
							msg.getExecutionHistory());

			/*
			 * Compute a set of destinations for the votes, and sends out the
			 * votes to all replicas <i>that replicate objects modified inside
			 * the transaction</i>.
			 */
			Set<String> dest = Partitioner.getInstance().resolveToGroupNames(
					msg.getExecutionHistory().getWriteSet().getKeys());
			rmStream.reliableMulticast(new VoteMessage(new Vote(msg
					.getExecutionHistory().getTransactionHandler(), certified,
					myGroup, msg.dest), dest));

			/*
			 * If the transaction is read-only, and the processing thread is not
			 * the coordinator, it has nothing else to do. It can returns
			 * without knowing the <i>outcome</i> .
			 * 
			 * Those processes that receive votes, will decide the outcome of
			 * the transactionm and send the results to the coordinator.
			 * 
			 * <p> It just garbage collect, and returns true.
			 */
			if (msg.getExecutionHistory().getTransactionType() == TransactionType.READONLY_TRANSACTION
					&& !msg.gSource.equals(myGroup)) {
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
			if (msg.dest.size() == 1
					&& msg.dest.contains(Membership.getInstance().myGroup()
							.name())) {
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
					.getExecutionHistory().getTransactionState(), msg
					.getExecutionHistory().isCertifyAtCoordinator());
			handleTerminationResult(terminationResult);
			return true;
		}
	}
}
