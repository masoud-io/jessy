package fr.inria.jessy;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import fr.inria.jessy.transaction.TransactionHandler;

public class DistributedTermination implements Learner,Runnable {

	private static DistributedTermination instance;
	static {
		instance = new DistributedTermination();
	}

	public static DistributedTermination getInstance() {
		return instance;
	}

	private ExecutorPool pool = ExecutorPool.getInstance();
	private WanAMCastStream amStream;
	private ConcurrentMap<TransactionHandler, Future<Boolean>> futures;
	private ConcurrentMap<TransactionHandler, Boolean> terminationResults;
	private LinkedBlockingQueue<TerminateTransactionMessage> TerminateTransactionMessages;

	private DistributedTermination() {
		Membership.getInstance().getOrCreateTCPGroup("ALLNODES");
		amStream = FractalManager.getInstance().getOrCreateWanAMCastStream(
				"DistributedTerminationStream",
				Membership.getInstance().myGroup().name());
		amStream.registerLearner("TerminateTransactionMessage", this);

		futures = new ConcurrentHashMap<TransactionHandler, Future<Boolean>>();
		terminationResults = new ConcurrentHashMap<TransactionHandler, Boolean>();
		TerminateTransactionMessages = new LinkedBlockingQueue<TerminateTransactionMessage>();
	}

	public Future<Boolean> terminateTransaction(ExecutionHistory ex) {
		Future<Boolean> reply = pool.submit(new TerminateTransactionTask(ex));
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
		while(true){
			TerminateTransactionMessage msg=TerminateTransactionMessages.
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
	}

	class TerminateTransactionTask implements Callable<Boolean> {

		private ExecutionHistory executionHistory;

		public ExecutionHistory getExecutionHistory() {
			return executionHistory;
		}

		private TerminateTransactionTask(ExecutionHistory eh) {
			this.executionHistory = eh;
		}

		public Boolean call() throws Exception {
			HashSet<String> destGroups = new HashSet<String>();
			Set<String> concernedKeys = ConsistencyFactory
					.getConcerningKeys(executionHistory);

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
