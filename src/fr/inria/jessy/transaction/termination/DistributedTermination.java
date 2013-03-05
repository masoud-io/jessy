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
import org.jboss.netty.channel.Channel;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.communication.TerminationCommunicationFactory;
import fr.inria.jessy.communication.UnicastLearner;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.consistency.Consistency;
import fr.inria.jessy.consistency.Consistency.ConcernedKeysTarget;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;

/**
 * This class is responsible for sending transactions to remote replicas,
 * receiving the certification votes from remote replicas, deciding the outcome
 * of the transaction, and finally applying the transaction changes to the local
 * store.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class DistributedTermination implements Learner, UnicastLearner {

	protected static Logger logger = Logger
			.getLogger(DistributedTermination.class);

	protected static ValueRecorder certificationTime_readonly,
			certificationTime_update, certificationQueueingTime, applyingTransactionQueueingTime , votingTime, castLatency;
	
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
	 * Atomically delivered but not processed transactions. This list is to ensure the certification safety and 
	 * the safety of applying transactions to the data store.
	 * Thus, two transaction from this list can be certified and perform voting phase, if they do not have any conflict with
	 * all other concurrent transactions having been delivered before it as long as 
	 * {@link Consistency#certificationCommute(ExecutionHistory, ExecutionHistory)} returns true.
	 * Moreover, every transaction in this queue can execute without waiting as long as 
	 * {@link Consistency#applyingTransactionCommute()} returns true.
	 *  
	 */
	private LinkedList<TerminateTransactionRequestMessage> atomicDeliveredMessages;
	
	/**
	 * Terminated transactions
	 */	
	private ConcurrentLinkedHashMap<UUID, Object> terminated;
	private static final Object dummyObject=new Object();

	private static boolean reInitProbes=true;
	
	static {
		initProbesForInsertion();
	}

	public static void initProbesForInsertion(){
		certificationTime_update = new ValueRecorder(
				"DistributedTermination#certificationTime_Insert(ms)");
		certificationTime_update.setFormat("%a");
		certificationTime_update.setFactor(1000000);	
		
		certificationQueueingTime = new ValueRecorder(
				"DistributedTermination#certificationQueueingTime_Insert(ms)");
		certificationQueueingTime.setFormat("%a");
		certificationQueueingTime.setFactor(1000000);
		
		applyingTransactionQueueingTime = new ValueRecorder(
				"DistributedTermination#applyingTransactionQueueingTime_Insert(ms)");
		applyingTransactionQueueingTime.setFormat("%a");
		applyingTransactionQueueingTime.setFactor(1000000);
		
		votingTime = new ValueRecorder(
				"DistributedTermination#votingTime(ms)");
		votingTime.setFormat("%a");
		
		castLatency = new ValueRecorder(
				"DistributedTermination#castLatency_Loading(ms)");
		castLatency.setFormat("%a");
	}
	
	public synchronized static void initProbesForExecution(){
		if (!reInitProbes) return;
		
		reInitProbes=false;
		System.out.println("Initializing Probes for execution...");

		certificationTime_readonly = new ValueRecorder(
				"DistributedTermination#certificationTime_readonly(ms)");
		certificationTime_readonly.setFormat("%a");
		certificationTime_readonly.setFactor(1000000);

		certificationTime_update = new ValueRecorder(
				"DistributedTermination#certificationTime_update(ms)");
		certificationTime_update.setFormat("%a");
		certificationTime_update.setFactor(1000000);	
		
		certificationQueueingTime = new ValueRecorder(
				"DistributedTermination#certificationQueueingTime_update(ms)");
		certificationQueueingTime.setFormat("%a");
		certificationQueueingTime.setFactor(1000000);
		
		applyingTransactionQueueingTime = new ValueRecorder(
				"DistributedTermination#applyingTransactionQueueingTime_update(ms)");
		applyingTransactionQueueingTime.setFormat("%a");
		applyingTransactionQueueingTime.setFactor(1000000);

		votingTime = new ValueRecorder(
				"DistributedTermination#votingTime(ms)");
		votingTime.setFormat("%a");
		
		castLatency = new ValueRecorder(
				"DistributedTermination#castLatency_Update_ReadOnly(ms)");
		castLatency.setFormat("%a");

	}
	
	ApplyTransactionsToDataStore applyTransactionsToDataStore;
	
	public DistributedTermination(DistributedJessy j) {
		jessy = j;
		group = JessyGroupManager.getInstance().getMyGroup();
		
		terminationCommunication=TerminationCommunicationFactory.initAndGetConsistency(group, this, this);
		logger.info("initialized");

		terminationRequests = new ConcurrentHashMap<UUID, TransactionHandler>();
		atomicDeliveredMessages = new LinkedList<TerminateTransactionRequestMessage>();
		
		votingQuorums = new ConcurrentHashMap<TransactionHandler, VotingQuorum>();

		terminated = new ConcurrentLinkedHashMap.Builder<UUID, Object>()
				.maximumWeightedCapacity(ConstantPool.JESSY_TERMINATED_TRANSACTIONS_LOG_SIZE)
				.build();
		
		if (!jessy.getConsistency().applyingTransactionCommute()){
			applyTransactionsToDataStore=new ApplyTransactionsToDataStore(this);
			pool.submit(applyTransactionsToDataStore);
		}
			
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


	
	@Override
	public void receiveMessage(Object message, Channel channel) {
		if (message instanceof VoteMessage){
			voteMessageRM_Delivered(message);
		}
		else{
			logger.error("Netty delivered an unexpected message");
		}
	}
	
	/**
	 * Call back by Fractal upon receiving atomically delivered
	 * {@link TerminateTransactionRequestMessage} or {@link Vote}.
	 */
	@Deprecated
	public void learn(Stream s, Serializable v) {
		if (v instanceof TerminateTransactionRequestMessage) {
			TerminateTransactionMessageAM_Delivered((TerminateTransactionRequestMessage)v);
		} else if (v instanceof VoteMessage){ 
			voteMessageRM_Delivered(v);
		}
		else{
			logger.error("Fractal delivered an unexpected message");
		}

	}
	
	private void TerminateTransactionMessageAM_Delivered(TerminateTransactionRequestMessage terminateRequestMessage){
		castLatency.add(System.currentTimeMillis()-terminateRequestMessage.startCasting);
		if (reInitProbes){
			if (terminateRequestMessage.getExecutionHistory().getCreateSet()==null ||
					terminateRequestMessage.getExecutionHistory().getCreateSet().size()==0)
				initProbesForExecution();
		}
		
		if (ConstantPool.logging)
			logger.error("got a TerminateTransactionRequestMessage for "
				+ terminateRequestMessage.getExecutionHistory()
						.getTransactionHandler().getId() + " , read keys :" + terminateRequestMessage.getExecutionHistory().getReadSet().getKeys());

		terminateRequestMessage.getExecutionHistory()
				.setStartCertification(System.nanoTime());
		
		ConsistencyFactory.getConsistencyInstance().transactionDeliveredForTermination(terminateRequestMessage);
		
		try{
			synchronized (atomicDeliveredMessages) {				
				TransactionHandler abortedTransactionHandler=terminateRequestMessage.getExecutionHistory().getTransactionHandler().getPreviousAbortedTransactionHandler();
				if (abortedTransactionHandler!=null){
					for (TerminateTransactionRequestMessage req: atomicDeliveredMessages){
						if (req.getExecutionHistory().getTransactionHandler().equals(abortedTransactionHandler)){
							garbageCollectJessyReplica(req);
							if (applyTransactionsToDataStore!=null)
								applyTransactionsToDataStore.removeFromQueue(req);
							break;
						}
					}

				}
				atomicDeliveredMessages.offer(terminateRequestMessage);
			}
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
		
		
		pool.execute(new CertifyAndVoteTask(terminateRequestMessage));
	}
	
	/**
	 * Call upon receiving a new vote. Since the vote can be received
	 * via both Netty and Fractal, this method should be called from both
	 * {@link this#learn(Stream, Serializable)} and {@link this#receiveMessage(Object, Channel)}
	 * @param msg
	 */
	private void voteMessageRM_Delivered(Object msg){
		Vote vote = ((VoteMessage) msg).getVote();

		if (ConstantPool.logging)
			logger.debug("got a VoteMessage from " + vote.getVoterGroupName()
				+ " for " + vote.getTransactionHandler().getId());

		if (terminated.containsKey(vote.getTransactionHandler().getId()))
			return;
		addVote(vote);
	}

	private VotingQuorum getOrCreateVotingQuorums(TransactionHandler transactionHandler) {
		VotingQuorum vq = votingQuorums.putIfAbsent(
				transactionHandler,
				new VotingQuorum(transactionHandler));
		if (vq == null) {
			logger.debug("creating voting quorum for "
					+ transactionHandler);
			vq = votingQuorums.get(transactionHandler);
		}
		
		return vq;
	}
	
	/**
	 * Upon receiving a new certification vote, it is added to the
	 * votingQuorums.
	 * 
	 * @param vote
	 */
	private void addVote(Vote vote) {
		VotingQuorum vq = getOrCreateVotingQuorums(vote.getTransactionHandler());

		try {
			jessy.getConsistency().voteReceived(vote);
			
			vq.addVote(vote);

			if (JessyGroupManager.getInstance().isProxy()) {
				votingTime.add(System.currentTimeMillis()-vote.startVoteTime);
			}
		} catch (Exception ex) {
			/*
			 * If here is reached, it means that a concurrent thread has already
			 * garbage collected the voting quorum. Thus it has become null. <p> No special
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
	protected void handleTerminationResult(TerminateTransactionRequestMessage msg)
			throws Exception {


		ExecutionHistory executionHistory = msg.getExecutionHistory();

		TransactionHandler th = executionHistory.getTransactionHandler();
		assert !terminated.containsKey(th.getId());

		if (executionHistory.getTransactionState() == TransactionState.COMMITTED) {

			/*
			 * Prepare the transaction. I.e., update the vectors of modified
			 * entities.
			 */
			jessy.getConsistency().prepareToCommit(msg);

			/*
			 * Apply the modified entities.
			 */
			jessy.applyModifiedEntities(executionHistory);

		}

		/*
		 * We have to garbage collect at the server ASAP, because concurrent transactions can only
		 * proceed after garbage collecting the current delivered transaction.
		 */
		garbageCollectJessyReplica(msg);
		
		if (executionHistory.getTransactionState() == TransactionState.COMMITTED) {
			/*
			 * calls the postCommit method of the consistency criterion for post
			 * commit actions. (e.g., propagating vectors)
			 */
			jessy.getConsistency().postCommit(executionHistory);
		}

	}

	/**
	 * Garbage collect all concurrent hash maps entries for the given
	 * {@code transactionHandler}
	 * This method is executed by both Jessy instances (Jessy Replica and Jessy Proxy).
	 * 
	 * @param transactionHandler
	 *            The transactionHandler to be garbage collected.
	 */
	private void garbageCollectJessyInstance(TransactionHandler transactionHandler) {
		jessy.garbageCollectTransaction(transactionHandler);
		
		try{

			terminationRequests.remove(transactionHandler);
			votingQuorums.remove(transactionHandler);

			terminated.put(transactionHandler.getId(),dummyObject);
		}
		catch(Exception ex){
			System.out.println("Garbage collecting cannot be done!");
		}

	}
	
	/**
	 * Garbage collect the TerminateTransactionRequestMessage at the jessy replica. I.e., all jessy instances that 
	 * have delivered TerminateTransactionRequestMessage.
	 * 
	 * If jessy proxy does not replicate JessyEntities, this garbage collection should be executed at it.
	 * 
	 * @param TerminateTransactionRequestMessage message that should be garbage collected.
	 */
	private void garbageCollectJessyReplica(TerminateTransactionRequestMessage msg){
		try{
			synchronized (atomicDeliveredMessages) {
				atomicDeliveredMessages.remove(msg);
				atomicDeliveredMessages.notifyAll();
			}
		}
		catch (Exception ex){
			System.out.println("REMOVING FROM ATOMIC DELIVERED MESSAGES CANNOT BE DONE!");
			ex.printStackTrace();
		}

		garbageCollectJessyInstance(msg.getExecutionHistory().getTransactionHandler());
	}
	
	public void closeConnections(){
		terminationCommunication.close();
	}
	
	/**
	 * Runs at the transaction Coordinator upon receiving a transaction for
	 * termination. It first gets destination groups for atomic
	 * multicast/broadcast, and then cast a
	 * {@link TerminateTransactionRequestMessage} to the destination groups. If
	 * the destination group is empty, it means that the transaction can commit
	 * right away without further synchronization. For example, in case of NMSI,
	 * SI, US, or RC, read-only transaction can commit right away without
	 * synchronization.
	 * 
	 * @author Masoud Saeida Ardekani
	 * 
	 */
	private class AtomicMulticastTask implements Callable<TransactionState> {

		private ExecutionHistory executionHistory;

		private AtomicMulticastTask(ExecutionHistory eh) {
			this.executionHistory = eh;
		}

		public TransactionState call() throws Exception {

			TransactionState result;

			Set<String> concernedKeys = jessy.getConsistency()
					.getConcerningKeys(executionHistory,
							ConcernedKeysTarget.TERMINATION_CAST);

			/*
			 * If there is no concerning key, it means that the transaction can
			 * commit right away. e.g. read-only transaction with NMSI
			 * consistency.
			 */
			if (concernedKeys.size() == 0) {

				executionHistory.changeState(TransactionState.COMMITTED);
				result = TransactionState.COMMITTED;

			} else {

				if (ConstantPool.logging)
					if (executionHistory.getTransactionType()==TransactionType.UPDATE_TRANSACTION){
						logger.debug("***Staring certification of " + executionHistory.getTransactionHandler().getId());
					}
				
				HashSet<String> destGroups = new HashSet<String>();
				
				destGroups
						.addAll(jessy.partitioner.resolveNames(concernedKeys));
				if (destGroups.contains(group.name())) {
					executionHistory.setCertifyAtCoordinator(true);
				} else {
					int coordinatorSwid=JessyGroupManager.getInstance()
							.getSourceId();
					executionHistory.setCertifyAtCoordinator(false);
					executionHistory.setCoordinatorSwid(coordinatorSwid);
					executionHistory.setCoordinatorHost(JessyGroupManager.getInstance().getMembership()
							.adressOf(coordinatorSwid));
				}

				votingQuorums.put(
						executionHistory.getTransactionHandler(),
						new VotingQuorum(executionHistory
								.getTransactionHandler()));

				/*
				 * gets the pointer for the transaction's VotingQuorum because
				 * the votingQuorums might be garbage collected by another
				 * thread after multicasting this transaction.
				 */
				VotingQuorum vq = votingQuorums.get(executionHistory
						.getTransactionHandler());

				if (ConstantPool.logging)
					logger.debug("A node in Group " + group
						+ " send a termination message "
						+ executionHistory.getTransactionHandler().getId()
						+ " to " + destGroups);
				/*
				 * Atomic multicast the transaction.
				 */
				executionHistory.clearReadValues();
				terminationCommunication
						.terminateTransaction(executionHistory, destGroups, group
												.name(), JessyGroupManager
												.getInstance().getSourceId());

				/*
				 * Wait here until the result of the transaction is known.
				 */
				result = vq.waitVoteResult(jessy.getConsistency().getVotersToCoordinator(destGroups,executionHistory));
				if (result==TransactionState.ABORTED_BY_TIMEOUT){
					logger.error("Abort by timeout from votingQ " + executionHistory.getTransactionHandler());
				}

			}

			if (!executionHistory.isCertifyAtCoordinator()) {
				garbageCollectJessyInstance(executionHistory.getTransactionHandler());
			}
			
			if (ConstantPool.logging)
				if (executionHistory.getTransactionType()==TransactionType.UPDATE_TRANSACTION){
					logger.debug("***FINISHING certification of " + executionHistory.getTransactionHandler().getId());
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
							if (!jessy.getConsistency().certificationCommute(n.getExecutionHistory(), msg.getExecutionHistory())) 
							{
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
				
				certificationQueueingTime.add(System.nanoTime()-msg.getExecutionHistory().getStartCertification());

				if (ConstantPool.logging)
					if (msg.getExecutionHistory().getTransactionType()==TransactionType.UPDATE_TRANSACTION){
						logger.error("Staring certification of " + msg.getExecutionHistory().getTransactionHandler().getId());
					}
				
				jessy.setExecutionHistory(msg.getExecutionHistory());

				Vote vote = jessy.getConsistency().createCertificationVote(
						msg.getExecutionHistory(), msg.getComputedObjectUponDelivery());
				

				/*
				 * Computes a set of destinations for the votes, and sends out
				 * the votes to all replicas <i>that replicate objects modified
				 * inside the transaction</i>. The group this node belongs to is
				 * omitted.
				 * 
				 * <p>
				 * 
				 * The votes will be sent to all concerned keys. Note that the
				 * optimization to only send the votes to the nodes replicating
				 * objects in the write set is not included. Thus, for example,
				 * under serializability, a node may wait to receive the votes
				 * from all nodes replicating the concerned keys, and then
				 * returns without performing anything.
				 */
				Set<String> voteReceivers =	jessy.partitioner.resolveNames(jessy
						.getConsistency().getConcerningKeys(
								msg.getExecutionHistory(),
								ConcernedKeysTarget.RECEIVE_VOTES));
				
				
				Set<String> voteSenders =jessy.partitioner.resolveNames(jessy
						.getConsistency().getConcerningKeys(
								msg.getExecutionHistory(),
								ConcernedKeysTarget.SEND_VOTES)); 

				/*
				 * if true, it means that it must wait for the vote from the
				 * others and apply the changes, otherwise, it only needs to
				 * send its vote, and garbage collect. For example, in SER, an
				 * instance which only replicates an object read by the
				 * transaction should send its vote, and return.
				 */
				boolean voteReceiver = voteReceivers.contains(group.name());
				msg.getExecutionHistory().setVoteReceiver(voteReceiver);
				boolean voteSender=voteSenders.contains(group.name());
				
				if (voteSender){

					voteReceivers.remove(group.name());
					vote.startVoteTime=System.currentTimeMillis();
					VoteMessage voteMsg = new VoteMessage(vote, voteReceivers,
							group.name(), JessyGroupManager.getInstance()
									.getSourceId());

					terminationCommunication.sendVote(voteMsg, msg
							.getExecutionHistory().isCertifyAtCoordinator(),
							msg.getExecutionHistory().getCoordinatorSwid(),msg.getExecutionHistory().getCoordinatorHost());
					
					
					/*
					 * we can garbage collect right away, and exit.
					 * if jessy replica is in RS(T) and not in WS(T), under SER, it should exit right away once it sends our votes. 
					 */
					if (!voteReceiver){
						measureCertificationTime(msg);
						garbageCollectJessyReplica(msg);
						return;
					}

				}

				if (voteReceiver) {
					
					if (voteSenders.contains(group.name()))
						addVote(vote);
					else
						getOrCreateVotingQuorums(vote.getTransactionHandler());
					
					TransactionState state = votingQuorums.get(
							msg.getExecutionHistory().getTransactionHandler())
							.waitVoteResult(voteSenders);

					msg.getExecutionHistory().changeState(state);
					
					if (ConstantPool.logging)
						logger.debug("got voting quorum for " + msg.getExecutionHistory().getTransactionHandler()
								+ " , result is " + state);

					/*
					 * we can garbage collect right away, and exit.
					 */
					if (state==TransactionState.ABORTED_BY_VOTING || state ==TransactionState.ABORTED_BY_TIMEOUT){
						jessy.getConsistency().postAbort(msg,vote);
						measureCertificationTime(msg);
						garbageCollectJessyReplica(msg);
						return;
					}

				}
				
				msg.getExecutionHistory().setApplyingTransactionQueueingStartTime(System.nanoTime());
				
				if (!jessy.getConsistency().applyingTransactionCommute() && voteSender && voteReceiver)
				{
					/*
					 * When applying transactions to the data-store does not commute, instead of waiting in this thread
					 * until the condition holds, we add the transaction to a queue, return right away, and only ONE thread applies the transactions in FIFO order.
					 * According to measurements, when update ratio is high, this solution improves the performance significantly.
					 * 
					 * Should be here in case this transaction modifying an object replicated here: NMSI-GMUVector, SI, PSI, US
					 */
					applyTransactionsToDataStore.addToQueue(msg);
					return;
				}
				
				measureApplyingTransactionQueueingTime(msg);
				
				handleTerminationResult(msg);

				measureCertificationTime(msg);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
	
	protected static void measureCertificationTime(TerminateTransactionRequestMessage msg){
		if (msg.getExecutionHistory().getTransactionType() == TransactionType.READONLY_TRANSACTION)
			certificationTime_readonly.add(System.nanoTime()
					- msg.getExecutionHistory()
					.getStartCertification());
		else if (msg.getExecutionHistory().isVoteReceiver() && (msg.getExecutionHistory().getTransactionType() == TransactionType.UPDATE_TRANSACTION))
			certificationTime_update.add(System.nanoTime()
					- msg.getExecutionHistory()
					.getStartCertification());
	}
	
	protected static void measureApplyingTransactionQueueingTime(TerminateTransactionRequestMessage msg){	
		if (msg.getExecutionHistory().isVoteReceiver())
			applyingTransactionQueueingTime.add(System.nanoTime()-msg.getExecutionHistory().getApplyingTransactionQueueingStartTime());
	}

}
