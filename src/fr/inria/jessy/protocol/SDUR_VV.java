package fr.inria.jessy.protocol;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.ExecutorPool;

import org.apache.log4j.Logger;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.ATOMIC_COMMIT_TYPE;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.MessagePropagation;
import fr.inria.jessy.communication.message.ParallelSnapshotIsolationPropagateMessage;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.consistency.US;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.termination.vote.Vote;
import fr.inria.jessy.transaction.termination.vote.VotePiggyback;
import fr.inria.jessy.transaction.termination.vote.VotingQuorum;
import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.GMUVector;
import fr.inria.jessy.vector.ValueVector.ComparisonResult;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VersionVector;

/**
 * SDUR implementation according to [SciasciaDSN2012] paper. 
 * It uses Walter vector implementation, and differs it by extending SER and not PSI.
 * 
 * CONS: SER
 * Vector: VersionVector
 * Atomic Commitment: GroupCommunication without acyclic
 * 
 * Note that although this class implements SER, the rules are like US, hence it extends US class. 
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class SDUR_VV extends US implements Learner {

	private ExecutorPool pool = ExecutorPool.getInstance();

	private static Logger logger = Logger
			.getLogger(SDUR_VV.class);

	private static AtomicInteger tempSequenceNumber=new AtomicInteger(0);
	
	static {
		votePiggybackRequired = true;
		READ_KEYS_REQUIRED_FOR_COMMUTATIVITY_TEST=false;
		ConstantPool.ATOMIC_COMMIT=ATOMIC_COMMIT_TYPE.GROUP_COMMUNICATION;
	}

	private MessagePropagation propagation;
	
	private HashMap<String,VersionVectorApplyPiggyback> applyPiggyback;

	private ConcurrentHashMap<UUID, VersionVectorPiggyback> receivedPiggybacks;
	
	/**
	 * This dequeue contains all transactions committed.
	 * It is required for the additional test in {@link this#certify(ExecutionHistory)}.
	 * 
	 * More precisely, it serves the need for execution of line 39 in Algorithm 4. 
	 */
	public static LinkedBlockingDeque<ExecutionHistory> committedTransactions;

	public SDUR_VV(JessyGroupManager m, DataStore store) {
		super(m, store);
		receivedPiggybacks = new ConcurrentHashMap<UUID, VersionVectorPiggyback>();
		propagation = new MessagePropagation(this,m);
		
		applyPiggyback=new HashMap<String, VersionVectorApplyPiggyback>();
		
		
		committedTransactions=new LinkedBlockingDeque<ExecutionHistory>(ConstantPool.SDUR_COMMITTED_TRANSACTIONS_SIZE);
		
		for(Group group:manager.getReplicaGroups()){
			VersionVectorApplyPiggyback task=new VersionVectorApplyPiggyback();
			pool.submit(task);
			applyPiggyback.put(group.name(),task);
		}
	}

	@Override
	public boolean applyingTransactionCommute() {
		return true;
	}

	/**
	 * @inheritDoc
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean certify(ExecutionHistory executionHistory) {
		TransactionType transactionType = executionHistory.getTransactionType();

		/*
		 * if the transaction is a read-only transaction, it commits right away.
		 */
		if (transactionType == TransactionType.READONLY_TRANSACTION) {
			
			if (ConstantPool.logging)
				logger.debug(executionHistory.getTransactionHandler() + " >> "
						+ transactionType.toString() + " >> COMMITTED");
			return true;
		}

		/*
		 * if the transaction is an init transaction, it first
		 * increments the vectors and then commits.
		 */
		if (transactionType == TransactionType.INIT_TRANSACTION) {

			// executionHistory.getWriteSet().addEntity(
			// executionHistory.getCreateSet());

			if (ConstantPool.logging)
				logger.debug(executionHistory.getTransactionHandler() + " >> "
						+ transactionType.toString()
						+ " >> INIT_TRANSACTION COMMITTED");
			return true;
		}

		/*
		 * If the transaction is not read-only or init, we consider the create
		 * operations as update operations. Thus, we move them to the writeSet
		 * List.
		 */
		executionHistory.getWriteSet().addEntity(
				executionHistory.getCreateSet());

		/*
		 * Line 39 of Algorithm 4
		 */
		CompactVector<String> transactionSnapshot=executionHistory.getReadSet().getCompactVector();
		ExecutionHistory pendingExecutionHistory;		
		Iterator<ExecutionHistory> itr= committedTransactions.iterator();
		while  (itr.hasNext()){
			pendingExecutionHistory=itr.next();
			
			if (pendingExecutionHistory.getReadSet().getCompactVector().compareTo(transactionSnapshot)==ComparisonResult.LOWER_THAN){
				break;
			}
			
			boolean result=true;
			if (executionHistory.getReadSet()!=null && pendingExecutionHistory.getWriteSet()!=null){
				result = !CollectionUtils.isIntersectingWith(pendingExecutionHistory.getWriteSet()
						.getKeys(), executionHistory.getReadSet().getKeys());
			}
			if (executionHistory.getWriteSet()!=null && pendingExecutionHistory.getReadSet()!=null){
				result = result && !CollectionUtils.isIntersectingWith(executionHistory.getWriteSet()
						.getKeys(), pendingExecutionHistory.getReadSet().getKeys());
			}			
			if (!result)
				return false;
		}
		
		JessyEntity lastComittedEntity;
		for (JessyEntity tmp : executionHistory.getReadSet().getEntities()) {

			try {

				lastComittedEntity = store
						.get(new ReadRequest<JessyEntity>(
								(Class<JessyEntity>) tmp.getClass(),
								"secondaryKey", tmp.getKey(), null))
						.getEntity().iterator().next();

				if (lastComittedEntity.getLocalVector().isCompatible(
						tmp.getLocalVector()) != Vector.CompatibleResult.COMPATIBLE) {

					if (ConstantPool.logging)
						logger.error("Aborting a transaction because for key " + tmp.getKey() + "local vector is "
								+ tmp.getLocalVector()
								+ " and last committed is "
								+ lastComittedEntity.getLocalVector() + " for transaction" + executionHistory.getTransactionHandler());

					return false;
				}

			} catch (NullPointerException e) {
				// nothing to do.
				// the key is simply not there.
			}

		}
		
		
		/*
		 * We need to know all concurrent transactions. 
		 * We add them here.  
		 */
		if (committedTransactions.size() > ConstantPool.SDUR_COMMITTED_TRANSACTIONS_SIZE-100){
			committedTransactions.removeLast();
		}
		committedTransactions.add(executionHistory);
		
		if (ConstantPool.logging)
			logger.debug(executionHistory.getTransactionHandler() + " >> "
					+ transactionType.toString() + " >> COMMITTED");
		return true;
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		ExecutionHistory executionHistory=msg.getExecutionHistory();
		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
			return;

		try {
			VersionVectorPiggyback pb;
			if (!receivedPiggybacks.keySet().contains(
					executionHistory.getTransactionHandler().getId())) {
				/*
				 * Trying to commit a transaction without receiving the sequence
				 * number. Something is wrong. Because we should have already
				 * received the vote from the WCoordinator, and along with the
				 * vote, we should have received the sequence number.
				 */
				logger.error("Preparing to commit without receiving the piggybacked message from WCoordinator " + msg.getExecutionHistory().getTransactionHandler().getId());
				System.exit(0);
			}

			/*
			 * Get and remove the piggyback sequence number. We do not need it
			 * anymore.
			 */
			pb = receivedPiggybacks.get(executionHistory
					.getTransactionHandler().getId());

			if (executionHistory.getTransactionType() == TransactionType.INIT_TRANSACTION) {
				executionHistory.getWriteSet().addEntity(
						executionHistory.getCreateSet());

				/*
				 * Init transaction sequence number always remains zero. Thus,
				 * all init values are zero.
				 */
				pb = new VersionVectorPiggyback(manager
						.getMyGroup().name(), 0, executionHistory);
				receivedPiggybacks.put(executionHistory.getTransactionHandler()
						.getId(), pb);
			}

			/*
			 * Wait until its conditions holds true, and then update the
			 * CommittedVTS
			 */
			applyPiggyback.get(pb.getwCoordinatorGroupName()).syncApply(pb);

			/*
			 * updatedVector is a new vector. It will be used as a new vector
			 * for all modified vectors.
			 * 
			 * <p> The update takes place according to the Walter system [Serrano2011]
			 */

			int seqNo=pb.getSequenceNumber();
			VersionVector<String> updatedVector = new VersionVector<String>(
					pb.getwCoordinatorGroupName(), seqNo);

			for (JessyEntity entity : executionHistory.getWriteSet()
					.getEntities()) {
				entity.setLocalVector(updatedVector.clone());
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void postCommit(ExecutionHistory executionHistory) {

		/*
		 * only the WCoordinator propagates the votes as in [Serrano11]
		 * 
		 * Read-only transaction does not propagate
		 */
		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION
				|| !isCoordinator(executionHistory))
			return;

		Set<String> alreadyNotified = new HashSet<String>();
		Set<String> dest = new HashSet<String>();

		alreadyNotified.addAll(manager.getPartitioner().resolveNames(
				getConcerningKeys(executionHistory,
						ConcernedKeysTarget.RECEIVE_VOTES)));

		/*
		 * Compute the set of jessy groups that have not receive the vector.
		 * I.e., those groups that are not concerned by the transaction.
		 */
		for (Group group : manager.getReplicaGroups()) {
			if (!alreadyNotified.contains(group.name())) {
				dest.add(group.name());
			}
		}

		VersionVectorPiggyback pb = receivedPiggybacks
				.remove(executionHistory.getTransactionHandler().getId());

		if (dest.size() > 0) {
			ParallelSnapshotIsolationPropagateMessage msg = new ParallelSnapshotIsolationPropagateMessage(
					pb, dest, manager.getMyGroup().name(),
					manager.getSourceId());			
			propagation.propagate(msg);
		}
	}
	
	/**
	 * If the transaction is aborted, then send the piggyback to 
	 * others, so they can update their commitVTS.
	 * Otherwise, the execution will halt because they cannot apply newly received piggybacks
	 * since this transaction's piggyback is missing.
	 * 
	 * For its self, it simply calls the learn method, and doesn't go through the network layer
	 * because of performance issues.
	 */
	@Override
	public void postAbort(TerminateTransactionRequestMessage msg, Vote vote){
		ExecutionHistory executionHistory=msg.getExecutionHistory();
		
		if (!isCoordinator(executionHistory))
			return;
		
		VersionVectorPiggyback pb = (VersionVectorPiggyback) vote
						.getVotePiggyBack().getPiggyback();
		
		Set<String> dest = new HashSet<String>();
		for (Group group : manager.getReplicaGroups()){
			if (!manager.getMyGroup().name().equals(group.name()))
				dest.add(group.name());
		}
		
		ParallelSnapshotIsolationPropagateMessage propagateMsg = new ParallelSnapshotIsolationPropagateMessage(
				pb, dest, manager.getMyGroup().name(),
				manager.getSourceId());
		/*
		 * Send to every other groups except myself
		 */
		propagation.propagate(propagateMsg);
		

		/*
		 * send to myself
		 */
		learn(null, propagateMsg);
		
	}

	/**
	 * @inheritDoc
	 * 
	 *             Receiving VersionVectors from different jessy instances.
	 *             <p>
	 *             upon receiving a Vector, update the VersionVector associated
	 *             with each jessy instance with the received vector.
	 */
	@Override
	public void learn(Stream s, Serializable v) {
		if (v instanceof ParallelSnapshotIsolationPropagateMessage) {
			ParallelSnapshotIsolationPropagateMessage msg = (ParallelSnapshotIsolationPropagateMessage) v;
			applyPiggyback.get(msg.getParallelSnapshotIsolationPiggyback().getwCoordinatorGroupName()).asyncApply(msg.getParallelSnapshotIsolationPiggyback());

		}
	}

	@Override
	public boolean transactionDeliveredForTermination(ConcurrentLinkedHashMap<UUID, Object> terminatedTransactions, ConcurrentHashMap<TransactionHandler, VotingQuorum>  quorumes, TerminateTransactionRequestMessage msg){
		try{
			
			/*
			 * Since this implementation is using Walter vectors, only the coordinator assigns the sequence number. 
			 * 
			 */
			if (isCoordinator(msg.getExecutionHistory())) {
				int sequenceNumber=0;

				if (msg.getExecutionHistory().getTransactionType()!=TransactionType.INIT_TRANSACTION)
					sequenceNumber=tempSequenceNumber.incrementAndGet();

				msg.setComputedObjectUponDelivery(new Integer(sequenceNumber));
			}
			
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
		return true;
	}
	
	/**
	 * @inheritDoc
	 */
	@Override
	public Vote createCertificationVote(ExecutionHistory executionHistory, Object object) {

		boolean isAborted = executionHistory.getTransactionType() == BLIND_WRITE
				|| certify(executionHistory);

		/*
		 * Create the piggyback vote if this instance is member of a group where
		 * the first write is for.
		 */
		VotePiggyback vp = null;
		if (isCoordinator(executionHistory)) {

			int sequenceNumber=(Integer)object;

			vp = new VotePiggyback(new VersionVectorPiggyback(
					manager.getMyGroup().name(), sequenceNumber,
					executionHistory));
		}

		return new Vote(executionHistory.getTransactionHandler(), isAborted,
				manager.getMyGroup().name(), vp);
	}

	/**
	 * Returns if the first write operation of the transaction is on an entity
	 * replicated by the local jessy instance. If so, this instance is called
	 * <i>WCoordinator</i> of the transaction, and is responsible for
	 * piggybacking new sequence number on top of its votes.
	 * 
	 * <p>
	 * Note that the first read cannot play this role because it might not write
	 * on the same object, thus won't receive the vote request during
	 * certification.
	 * 
	 * @param executionHistory
	 * @return
	 */
	private boolean isCoordinator(ExecutionHistory executionHistory) {

		String key;
		
		if (executionHistory.getWriteSet() == null && executionHistory.getCreateSet()==null){
			key = executionHistory.getReadSet().getKeys().iterator().next();
			if (manager.getPartitioner().isLocal(key)) {
				return true;
			}
		}
		else{
			if (executionHistory.getWriteSet().size() > 0) {
				key = executionHistory.getWriteSet().getKeys().iterator().next();
				if (manager.getPartitioner().isLocal(key)) {
					return true;
				}
			}
			
			if (executionHistory.getCreateSet().size() > 0) {
				key = executionHistory.getCreateSet().getKeys().iterator().next();
				if (manager.getPartitioner().isLocal(key)) {
					return true;
				}
			}
			
		}

		return false;

	}

	/**
	 * @inheritDoc
	 */
	public void voteReceived(Vote vote) {
		if (vote.getVotePiggyBack() != null)
			receivedPiggybacks.put(vote.getTransactionHandler().getId(),
					(VersionVectorPiggyback) vote
							.getVotePiggyBack().getPiggyback());
	}
	
}
