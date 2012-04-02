package fr.inria.jessy.transaction.termination;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionReplyMessage;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.termination.message.VoteMessage;

//TODO COMMENT ME
//TODO JESSY define pattern is crappy!
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
	private CopyOnWriteArrayList<TerminateTransactionRequestMessage> processingMessages;

	private String myGroup = Membership.getInstance().myGroup().name();

	public DistributedTermination(Jessy jessy) {
		this.jessy=jessy;
		
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
		processingMessages = new CopyOnWriteArrayList<TerminateTransactionRequestMessage>();
		votingQuorums = new ConcurrentHashMap<TransactionHandler, VotingQuorum>();
		coordinatorGroups = new ConcurrentHashMap<TransactionHandler, String>();
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
			handleTransactionTermination(replyMessage.getTerminationResult());
		} else {
			addVote((Vote) v);
		}

	}

	@Override
	public void run() {
		TerminateTransactionRequestMessage msg;

		while (true) {

			msg = atomicDeliveredMessages.peek();
			if (msg == null)
				continue;

			for (TerminateTransactionRequestMessage committing : processingMessages) {
				if (ConsistencyFactory.getConsistency().hasConflict(
						msg.getExecutionHistory(),
						committing.getExecutionHistory())) {
					continue;
				}
			}

			msg = atomicDeliveredMessages.poll();
			processingMessages.add(msg);
			pool.submit(new CertifyAndVoteTask(msg));

		}

	}

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
	private synchronized void handleTransactionTermination(
			TerminationResult terminationResult) {
		Future<TerminationResult> future = futures.get(terminationResult
				.getTransactionHandler());
		HashSet<String> cordinatorGroup = new HashSet<String>();
		cordinatorGroup.add(coordinatorGroups.get(terminationResult
				.getTransactionHandler()));

		assert (terminationResult.isSendBackToCoordinator() && cordinatorGroup
				.contains(myGroup));

		if (future != null) {
			terminationResults.put(terminationResult.getTransactionHandler(),
					terminationResult);
			future.notify();
		} else if (terminationResult.isSendBackToCoordinator()
				&& !cordinatorGroup.contains(myGroup)) {
			TerminateTransactionReplyMessage replyMessage = new TerminateTransactionReplyMessage(
					terminationResult, cordinatorGroup,myGroup);
			amStream.atomicMulticast(replyMessage, cordinatorGroup);
		}

		garbageCollect(terminationResult.getTransactionHandler());
	}

	private synchronized void garbageCollect(
			TransactionHandler transactionHandler) {
		if (futures.containsKey(transactionHandler)) {
			futures.remove(transactionHandler);
			terminationResults.remove(transactionHandler);
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
			 * multicast, certifyAtProxy is set to true, otherwise, it is set to
			 * false.
			 */
			if (destGroups.contains(myGroup))
				executionHistory.setCertifyAtCoordinator(true);
			else
				executionHistory.setCertifyAtCoordinator(false);

			amStream.atomicMulticast(new TerminateTransactionRequestMessage(
					executionHistory, destGroups,myGroup), destGroups);
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
			boolean outcome = ConsistencyFactory.getConsistency()
					.certify(jessy.getLastCommittedEntities(),
							msg.getExecutionHistory());

			/*
			 * Send the outcome to the write-sets
			 * 
			 * TODO It can be executed in a seperate thread!!! What about
			 * safety?
			 */
			{
				Set<String> dest = Partitioner.getInstance()
						.resolveToGroupNames(
								msg.getExecutionHistory().getWriteSet()
										.getKeys());
				rmStream.reliableMulticast(new VoteMessage(new Vote(msg
						.getExecutionHistory().getTransactionHandler(),
						outcome, myGroup, msg.dest), dest));
			}

			// /*
			// * If it is a read-only transaction, it has nothing else to do. It
			// * can commit.
			// */
			// if (msg.getExecutionHistory().getTransactionType() ==
			// TransactionType.READONLY_TRANSACTION) {
			// /*
			// * If future is not null, then the transaction is managed by
			// * localJessy, and it should be notified along with the result
			// * of the termination.
			// */
			// notifyFuture(TerminationResult.COMMITTED, msg
			// .getExecutionHistory().getTransactionHandler());
			// continue;
			// }

			/*
			 * if it holds all keys, it can decide <i>locally</i>. Otherwise, it
			 * should wait to receive votes;
			 */
			if (msg.dest.size() == 1
					&& msg.dest.contains(Membership.getInstance().myGroup()
							.name())) {
				msg.getExecutionHistory().changeState(
						TransactionState.COMMITTED);
			} else {
				TransactionState state = votingQuorums.get(
						msg.getExecutionHistory().getTransactionHandler())
						.getTerminationResult();
				msg.getExecutionHistory().changeState(state);
			}
			TerminationResult terminationResult = new TerminationResult(msg
					.getExecutionHistory().getTransactionHandler(), msg
					.getExecutionHistory().getTransactionState(), msg
					.getExecutionHistory().isCertifyAtCoordinator());
			handleTransactionTermination(terminationResult);
			processingMessages.remove(msg);
			return true;
		}
	}
}
