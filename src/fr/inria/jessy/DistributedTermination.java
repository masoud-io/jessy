package fr.inria.jessy;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.rmcast.WanMessage;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import utils.ExecutorPool;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionHandler;

//TODO COMMENT ME
//TODO GARBAGE COLLECT ME
public class DistributedTermination implements Learner, Runnable {

	private static DistributedTermination instance;
	private static Jessy jessy;

	public static DistributedTermination getInstance(Jessy j) {
		if (instance == null) {
			jessy = j;
			instance = new DistributedTermination();
		}
		return instance;
	}

	public enum TerminationResult {
		/**
		 * the transaction is committed
		 */
		COMMITTED,
		/**
		 * the transaction is aborted.
		 */
		ABORTED
	};

	private ExecutorPool pool = ExecutorPool.getInstance();
	private WanAMCastStream amStream;
	private ConcurrentMap<TransactionHandler, Future<TerminationResult>> futures;
	private ConcurrentMap<TransactionHandler, TerminationResult> terminationResults;
	private LinkedBlockingQueue<TerminateTransactionMessage> TerminateTransactionMessages;

	private DistributedTermination() {
		Membership.getInstance().getOrCreateTCPGroup("ALLNODES");
		amStream = FractalManager.getInstance().getOrCreateWanAMCastStream(
				"DistributedTerminationStream",
				Membership.getInstance().myGroup().name());
		amStream.registerLearner("TerminateTransactionMessage", this);

		futures = new ConcurrentHashMap<TransactionHandler, Future<TerminationResult>>();
		terminationResults = new ConcurrentHashMap<TransactionHandler, TerminationResult>();
		TerminateTransactionMessages = new LinkedBlockingQueue<TerminateTransactionMessage>();
	}

	public Future<TerminationResult> terminateTransaction(ExecutionHistory ex) {
		Future<TerminationResult> reply = pool
				.submit(new TerminateTransactionTask(ex));
		futures.put(ex.getTransactionHandler(), reply);
		return reply;
	}

	@SuppressWarnings("unchecked")
	@Deprecated
	public void learn(Stream s, Serializable v) {
		if (v instanceof TerminateTransactionMessage) {
			TerminateTransactionMessages.add((TerminateTransactionMessage) v);
		}

	}

	@Override
	public void run() {
		TerminateTransactionMessage msg;
		Boolean outcome;
		while (true) {
			msg = TerminateTransactionMessages.poll();
			if (msg == null)
				continue;

			outcome = ConsistencyFactory.getConsistency()
					.certify(jessy.getLastCommittedEntities(),
							msg.getExecutionHistory());

			// TODO SEND OUTCOME TO WRITE SETS****

			/*
			 * If it is a read-only transaction, it has nothing else to do. It
			 * can commit.
			 */
			if (msg.getExecutionHistory().getTransactionType() == TransactionType.READONLY_TRANSACTION) {
				/*
				 * If future is not null, then the transaction is managed by
				 * localJessy, and it should be notified along with the result
				 * of the termination.
				 */
				notifyFuture(TerminationResult.COMMITTED, msg
						.getExecutionHistory().getTransactionHandler());
				continue;
			}

			/*
			 * if it holds all keys, it can decide <i>locally</i>. Otherwise, it
			 * should wait to receive votes;
			 */
			if (msg.dest.size() == 1
					&& msg.dest.contains(Membership.getInstance().myGroup()
							.name())) {
				notifyFuture(TerminationResult.COMMITTED, msg
						.getExecutionHistory().getTransactionHandler());
			} else {

			}

		}

	}

	private void notifyFuture(TerminationResult terminationResult,
			TransactionHandler transactionHandler) {
		Future<TerminationResult> future = futures.get(transactionHandler);
		if (future != null) {
			terminationResults.put(transactionHandler, terminationResult);
			future.notify();
		}
	}

	public class TerminateTransactionMessage extends WanMessage {
		private static final long serialVersionUID = ConstantPool.JESSY_MID;
		ExecutionHistory executionHistory;

		// For Fractal
		public TerminateTransactionMessage() {
		}

		TerminateTransactionMessage(ExecutionHistory eh, Set<String> dest) {
			super(eh, dest, Membership.getInstance().myGroup().name(),
					Membership.getInstance().myId());
		}

		public ExecutionHistory getExecutionHistory() {
			return executionHistory;
		}

	}

	class TerminateTransactionTask implements Callable<TerminationResult> {

		private ExecutionHistory executionHistory;

		public ExecutionHistory getExecutionHistory() {
			return executionHistory;
		}

		private TerminateTransactionTask(ExecutionHistory eh) {
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
			if (concernedKeys.size() == 0)
				return TerminationResult.COMMITTED;

			destGroups.addAll(Partitioner.getInstance().resolveToGroupNames(
					concernedKeys));
			amStream.atomicMulticast(new TerminateTransactionMessage(
					executionHistory, destGroups), destGroups);
			futures.get(executionHistory.getTransactionHandler()).wait();
			return terminationResults.get(executionHistory
					.getTransactionHandler());
		}

	}

}
