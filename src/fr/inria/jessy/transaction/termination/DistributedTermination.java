package fr.inria.jessy.transaction.termination;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.ATOMIC_COMMIT_TYPE;
import fr.inria.jessy.DebuggingFlag;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.UnicastLearner;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.consistency.Consistency;
import fr.inria.jessy.consistency.Consistency.ConcernedKeysTarget;
import fr.inria.jessy.protocol.ProtocolFactory;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.vote.Vote;
import fr.inria.jessy.transaction.termination.vote.VotingQuorum;

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

	protected static ValueRecorder readOnlyCertificationLatency, updateCertificationLatency, 
								   certificationQueueingLatency, applyingTransactionQueueingLatency , votingLatency;
	
	protected DistributedJessy jessy;
	
	protected JessyGroupManager manager;

	protected ExecutorPool pool = ExecutorPool.getInstance();

	protected Group group;

	/**
	 * VotingQuorums for processing transactions.
	 */
	protected ConcurrentHashMap<TransactionHandler, VotingQuorum> votingQuorums;

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
	
	ApplyTransactionsToDataStore applyTransactionsToDataStore;
	
	BlockingQueue<TerminateTransactionRequestMessage> amQueue=new LinkedBlockingQueue<TerminateTransactionRequestMessage>();
	
	private AtomicCommit atomicCommit;
	private String voterName;
		
	static {
		readOnlyCertificationLatency = new ValueRecorder(
				"DistributedTermination#certificationTime_readonly(ms)");
		readOnlyCertificationLatency.setFormat("%a");

		updateCertificationLatency = new ValueRecorder(
				"DistributedTermination#certificationTime_update(ms)");
		updateCertificationLatency.setFormat("%a");
		
		certificationQueueingLatency = new ValueRecorder(
				"DistributedTermination#certificationQueueingTime_update(ms)");
		certificationQueueingLatency.setFormat("%a");
		
		applyingTransactionQueueingLatency = new ValueRecorder(
				"DistributedTermination#applyingTransactionQueueingTime_update(ms)");
		applyingTransactionQueueingLatency.setFormat("%a");

		votingLatency = new ValueRecorder(
				"DistributedTermination#votingTime(ms)");
		votingLatency.setFormat("%a");

	}
		
	public DistributedTermination(DistributedJessy j) {
		jessy = j;
		manager = j.manager;
		group = manager.getMyGroup();
		
		logger.info("initialized");

		atomicDeliveredMessages = new LinkedList<TerminateTransactionRequestMessage>();
		
		votingQuorums = new ConcurrentHashMap<TransactionHandler, VotingQuorum>();

		terminated = new ConcurrentLinkedHashMap.Builder<UUID, Object>()
				.maximumWeightedCapacity(ConstantPool.JESSY_TERMINATED_TRANSACTIONS_LOG_SIZE)
				.build();
		
		if (!jessy.getConsistency().applyingTransactionCommute()){
			applyTransactionsToDataStore=new ApplyTransactionsToDataStore(this);
			pool.submit(applyTransactionsToDataStore);
		}
		
		if (ConstantPool.ATOMIC_COMMIT==ATOMIC_COMMIT_TYPE.GROUP_COMMUNICATION){
			atomicCommit=new GroupCommunicationCommit(this);
			voterName=group.name();
		}
		else if (ConstantPool.ATOMIC_COMMIT==ATOMIC_COMMIT_TYPE.TWO_PHASE_COMMIT){
			atomicCommit=new TwoPhaseCommit(this);
			voterName="" + jessy.manager.getSourceId();
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
		ex.changeState(TransactionState.COMMITTING);
		Future<TransactionState> reply = pool
				.submit(new AtomicMulticastTask(ex));
		return reply;
	}

	@Override
	public void receiveMessage(Object message, Channel channel) {
		if (message instanceof VoteMessage){
			handleVoteMessage(message);
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
		try {
			if (v instanceof TerminateTransactionRequestMessage) {
				handleTerminateTransactionMessage((TerminateTransactionRequestMessage)v);
			} else if (v instanceof VoteMessage){ 
				handleVoteMessage(v);
			}
			else{
				logger.error("Fractal delivered an unexpected message");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	

	private void handleTerminateTransactionMessage(TerminateTransactionRequestMessage terminateRequestMessage){
		if (DebuggingFlag.DISTRIBUTED_TERMINATION)
			logger.info("got a TerminateTransactionRequestMessage for "+ terminateRequestMessage.getExecutionHistory().toString());

		terminateRequestMessage.getExecutionHistory()
				.setStartCertificationTime(System.currentTimeMillis());
		
		boolean certifyAndVote=ProtocolFactory.getProtocolInstance().transactionDeliveredForTermination(terminated, votingQuorums, terminateRequestMessage);
	
		try{
			synchronized (atomicDeliveredMessages) {
				
				/*
				 * If the previous transaction has been timed-out, this transaction will carry the id of it.
				 * All the effects of the previous transaction should be wiped out from the system. 
				 */
				TransactionHandler timedoutTransactionHandler=terminateRequestMessage.getExecutionHistory().getTransactionHandler().getPreviousTimedoutTransactionHandler();
				if (timedoutTransactionHandler!=null){
					for (TerminateTransactionRequestMessage req: atomicDeliveredMessages){
						if (req.getExecutionHistory().getTransactionHandler().equals(timedoutTransactionHandler)){
							garbageCollectJessyReplica(req);
							if (applyTransactionsToDataStore!=null)
								applyTransactionsToDataStore.removeFromQueue(req);
							break;
						}
					}

				}
				
				if (certifyAndVote)
					atomicDeliveredMessages.add(terminateRequestMessage);
			}
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
		
		if (certifyAndVote){
			pool.execute(new CertifyAndVoteTask(terminateRequestMessage));
		}
	}
	
	/**
	 * Call upon receiving a new vote. Since the vote can be received
	 * via both Netty and Fractal, this method should be called from both
	 * {@link this#learn(Stream, Serializable)} and {@link this#receiveMessage(Object, Channel)}
	 * @param msg
	 */
	private void handleVoteMessage(Object msg){
		Vote vote = ((VoteMessage) msg).getVote();

		if (ConstantPool.logging)
			logger.debug("got a VoteMessage from " + vote.getVoterEntityName()
				+ " for " + vote.getTransactionHandler().getId());

		addVote(vote);
	}

	private VotingQuorum getOrCreateVotingQuorums(TransactionHandler transactionHandler) {
		VotingQuorum vq = votingQuorums.putIfAbsent(
				transactionHandler,
				atomicCommit.getNewVotingQuorum(transactionHandler));
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
		if (terminated.containsKey(vote.getTransactionHandler().getId()))
			return;
		VotingQuorum vq = getOrCreateVotingQuorums(vote.getTransactionHandler());

		try {
			jessy.getConsistency().voteReceived(vote);
			vq.addVote(vote);
			jessy.getConsistency().voteAdded(vote.getTransactionHandler(), terminated, votingQuorums);
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

		try{
			if (DebuggingFlag.DISTRIBUTED_TERMINATION)
				logger.debug(msg.getExecutionHistory().toString());
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

			if (executionHistory.getTransactionState() == TransactionState.COMMITTED) {
				/*
				 * calls the postCommit method of the consistency criterion for post
				 * commit actions. (e.g., propagating vectors)
				 */
				jessy.getConsistency().postCommit(executionHistory);
			}

			/*
			 * We have to garbage collect at the server ASAP, because concurrent transactions can only
			 * proceed after garbage collecting the current delivered transaction.
			 */
			garbageCollectJessyReplica(msg);
		}
		catch (Exception ex ) 
		{
			ex.printStackTrace();
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
	public void garbageCollectJessyInstance(TransactionHandler transactionHandler) {
		jessy.garbageCollectTransaction(transactionHandler);
		
		try{

			terminated.put(transactionHandler.getId(),dummyObject);
			votingQuorums.remove(transactionHandler);

		}
		catch(Exception ex){
			System.out.println("Garbage collecting cannot be done!");
			ex.printStackTrace();
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
			logger.error("REMOVING FROM ATOMIC DELIVERED MESSAGES CANNOT BE DONE!");
			ex.printStackTrace();
		}

		garbageCollectJessyInstance(msg.getExecutionHistory().getTransactionHandler());
		ProtocolFactory.getProtocolInstance().garbageCollect(msg);
	}
	
	public void closeConnections(){
		atomicCommit.closeAtomicCommit();
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
			if (DebuggingFlag.DISTRIBUTED_TERMINATION && executionHistory.getTransactionType()==TransactionType.UPDATE_TRANSACTION)
				logger.info("Proxy starts certification of update transaction " + executionHistory.toString());
			

			TransactionState result=null;

			try {
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

					Set<String> destGroups=jessy.partitioner.resolveNames(concernedKeys);
					VotingQuorum vq=atomicCommit.broadcastTransaction(executionHistory, destGroups);
					/*
					 * Wait here until the result of the transaction is known.
					 */
					result = vq.waitVoteResult(jessy.getConsistency().getVotersToCoordinator(destGroups,executionHistory));
					
					if (DebuggingFlag.DISTRIBUTED_TERMINATION && result==TransactionState.ABORTED_BY_TIMEOUT)
						logger.error("Abort by timeout from votingQ " + executionHistory.getTransactionHandler());

				}

				
				if (!executionHistory.isCertifyAtCoordinator()) {
					garbageCollectJessyInstance(executionHistory.getTransactionHandler());
				}
				
				if (DebuggingFlag.DISTRIBUTED_TERMINATION) // && executionHistory.getTransactionType()==TransactionType.UPDATE_TRANSACTION)
					logger.error("Proxy finishes certification of transaction " + executionHistory.toString());
				
			} catch (Exception e) {
				e.printStackTrace();
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

			if (DebuggingFlag.DISTRIBUTED_TERMINATION)
				logger.error("Starting certification of " + msg.getExecutionHistory().toString());
			
			try {

				long start = System.currentTimeMillis();
				
				boolean preemptive_abort=!atomicCommit.proceedToCertifyAndVote(msg);
				
				certificationQueueingLatency.add(System.currentTimeMillis()-start);

				start = System.currentTimeMillis();
				
				jessy.setExecutionHistory(msg.getExecutionHistory());

				Vote vote =null;
				if (preemptive_abort){					
					vote=new Vote(msg.getExecutionHistory().getTransactionHandler(),false, group.name(), null);
				}
				else {
					vote = jessy.getConsistency().createCertificationVote(
						msg.getExecutionHistory(), msg.getComputedObjectUponDelivery());
				}
				

				/*
				 * if true, it means that it must wait for the vote from the
				 * others and apply the changes, otherwise, it only needs to
				 * send its vote, and garbage collect. For example, in SER, an
				 * instance which only replicates an object read by the
				 * transaction should send its vote, and return.
				 */
				AtomicBoolean voteReceiver=new AtomicBoolean(false);
				AtomicBoolean voteSender=new AtomicBoolean(false);

				Set<String> voteReceivers=new HashSet<String>();
				Set<String> voteSenders=new HashSet<String>();				
				atomicCommit.setVoters(msg, voteReceivers, voteReceiver, voteSenders, voteSender);

				if (DebuggingFlag.DISTRIBUTED_TERMINATION){
					logger.error(msg.getExecutionHistory().getTransactionHandler().toString() + " Vote Senders " + voteSenders + " Is sender " + voteSender.get() );
					logger.error(msg.getExecutionHistory().getTransactionHandler().toString() + " Vote Receivers " + voteReceivers + " Is Receiver " + voteReceiver.get() );
				}
				
				msg.getExecutionHistory().setVoteReceiver(voteReceiver.get());
				if (voteSender.get()==true){

					voteReceivers.remove(voterName);
					VoteMessage voteMsg = new VoteMessage(vote, voteReceivers,
							group.name(), manager.getSourceId());

					try{
						atomicCommit.sendVote(voteMsg, msg);
					}catch(Exception e){
						e.printStackTrace();
					}
					
					/*
					 * we can garbage collect right away, and exit.
					 * if jessy replica is in RS(T) and not in WS(T), under SER, it should exit right away once it sends our votes. 
					 */
					if (voteReceiver.get()==false){
						measureCertificationTime(msg);
						garbageCollectJessyReplica(msg);
						return;
					}

				}

				if (voteReceiver.get()==true) {
					
					if (DebuggingFlag.DISTRIBUTED_TERMINATION)
						logger.error("Needs to wait for the votes from " + voteSenders + "for " + msg.getExecutionHistory().getTransactionHandler().toString());
						
					if (voteSenders.contains(voterName))
						addVote(vote);
					else
						getOrCreateVotingQuorums(vote.getTransactionHandler());
					
					assert votingQuorums.containsKey(msg.getExecutionHistory().getTransactionHandler()) : msg.getExecutionHistory().getTransactionHandler() +"\n"+votingQuorums;
 					
					TransactionState state = votingQuorums.get(
							msg.getExecutionHistory().getTransactionHandler())
							.waitVoteResult(voteSenders);
					msg.getExecutionHistory().changeState(state);					
					jessy.getConsistency().quorumReached(msg, state);
					atomicCommit.quorumReached(msg,state, vote);
					
					if (DebuggingFlag.DISTRIBUTED_TERMINATION)
						logger.error("Got the votes for " + msg.getExecutionHistory().getTransactionHandler().getId() + " , result is " + state);

					votingLatency.add(System.currentTimeMillis()-start);
					
					/*
					 * we can garbage collect right away, and exit.
					 */
					if (state==TransactionState.ABORTED_BY_VOTING || state ==TransactionState.ABORTED_BY_TIMEOUT){
						
						if (DebuggingFlag.DISTRIBUTED_TERMINATION)
							logger.error("Aborting " + msg.getExecutionHistory().getTransactionHandler().toString());

						jessy.getConsistency().postAbort(msg,vote);
						measureCertificationTime(msg);
						garbageCollectJessyReplica(msg);
						return;
					}

				}
				msg.getExecutionHistory().setApplyingTransactionQueueingStartTime(System.currentTimeMillis());
				
				if (!jessy.getConsistency().applyingTransactionCommute() && voteSender.get() && voteReceiver.get())
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
			readOnlyCertificationLatency.add(System.currentTimeMillis()
					- msg.getExecutionHistory()
					.getStartCertificationTime());
		else if (msg.getExecutionHistory().isVoteReceiver() && (msg.getExecutionHistory().getTransactionType() == TransactionType.UPDATE_TRANSACTION))
			updateCertificationLatency.add(System.currentTimeMillis()
					- msg.getExecutionHistory()
					.getStartCertificationTime());
	}
	
	protected static void measureApplyingTransactionQueueingTime(TerminateTransactionRequestMessage msg){	
		if (msg.getExecutionHistory().isVoteReceiver())
			applyingTransactionQueueingLatency.add(System.currentTimeMillis()-msg.getExecutionHistory().getApplyingTransactionQueueingStartTime());
	}

	public LinkedList<TerminateTransactionRequestMessage> getAtomicDeliveredMessages(){
		return atomicDeliveredMessages;
	}
	
	public DistributedJessy getDistributedJessy(){
		return jessy;
	}
	
	public ConcurrentHashMap<TransactionHandler, VotingQuorum> getVotingQuorumes(){
		return votingQuorums;
	}
}
