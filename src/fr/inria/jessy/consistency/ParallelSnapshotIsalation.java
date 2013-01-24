package fr.inria.jessy.consistency;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.ExecutorPool;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.GenuineTerminationCommunication;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.MessagePropagation;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.communication.message.ParallelSnapshotIsolationPropagateMessage;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.termination.Vote;
import fr.inria.jessy.transaction.termination.VotePiggyback;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VersionVector;

/**
 * PSI implementation according to [Serrano2011] paper.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class ParallelSnapshotIsalation extends Consistency implements Learner {

	private ExecutorPool pool = ExecutorPool.getInstance();

	private static Logger logger = Logger
			.getLogger(ParallelSnapshotIsalation.class);

	static {
		votePiggybackRequired = true;
	}

	private MessagePropagation propagation;
	
	private HashMap<String,ParallelSnapshotIsolationApplyPiggyback> applyPiggyback;

	private ConcurrentHashMap<UUID, ParallelSnapshotIsolationPiggyback> receivedPiggybacks;

	public ParallelSnapshotIsalation(DataStore store) {
		super(store);
		receivedPiggybacks = new ConcurrentHashMap<UUID, ParallelSnapshotIsolationPiggyback>();
		propagation = new MessagePropagation(this);
		
		applyPiggyback=new HashMap<String, ParallelSnapshotIsolationApplyPiggyback>();
		
		for(Group group:JessyGroupManager.getInstance().getReplicaGroups()){
			ParallelSnapshotIsolationApplyPiggyback task=new ParallelSnapshotIsolationApplyPiggyback();
			pool.submit(task);
			applyPiggyback.put(group.name(),task);
		}
	}

	/**
	 * @inheritDoc
	 * 
	 *             According to the implementation of VersionVector, commute in
	 *             certification is not allowed.
	 *             <p>
	 *             Assume there are two servers s1 and s2 that replicate objects
	 *             x and y. Also assume there are two transaction t1 and t2 that
	 *             writes a new value on x and y accordingly. Since
	 *             commutativity may lead to execution of t1 and t2 in different
	 *             orders on s1 and s2, and version vector cannot distinguish
	 *             this re-ordering, it can lead to some strange behavior.
	 *             (i.e., reading inconsistent snapshots!)
	 *             
	 *             <p>
	 *             Note: if the group size is greater than 1, transactions cannot commute under any condition.
	 *             Because, sequenceNumber is assigned to a transaction in {@link Consistency#createCertificationVote(ExecutionHistory)}.
	 *             Now if two transactions T1 and T2 that does not have any conflict run in P1 and P2 (in group g1) concurrently,
	 *             then they can end up having different sequence numbers in different jessy instances.
	 *             For example, T1 and T2 can have sequenceNumbers 1 and 2 respectively in P1, and 
	 *             they can have sequenceNumbers 2 and 1 in P2 respectively. 
	 */
	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {

			return !CollectionUtils.isIntersectingWith(history1.getWriteSet()
					.getKeys(), history2.getWriteSet().getKeys());

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

		JessyEntity lastComittedEntity;
		for (JessyEntity tmp : executionHistory.getWriteSet().getEntities()) {

			try {

				lastComittedEntity = store
						.get(new ReadRequest<JessyEntity>(
								(Class<JessyEntity>) tmp.getClass(),
								"secondaryKey", tmp.getKey(), null))
						.getEntity().iterator().next();

				if (lastComittedEntity.getLocalVector().isCompatible(
						tmp.getLocalVector()) != Vector.CompatibleResult.COMPATIBLE) {

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
		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ transactionType.toString() + " >> COMMITTED");
		return true;
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {
		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
			return;

		try {
			ParallelSnapshotIsolationPiggyback pb;
			if (!receivedPiggybacks.keySet().contains(
					executionHistory.getTransactionHandler().getId())) {
				/*
				 * Trying to commit a transaction without receiving the sequence
				 * number. Something is wrong. Because we should have already
				 * received the vote from the WCoordinator, and along with the
				 * vote, we should have received the sequence number.
				 */
				logger.error("Preparing to commit without receiving the piggybacked message from WCoordinator");
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
				pb = new ParallelSnapshotIsolationPiggyback(manager
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
		 * only the WCoordinator propagate the votes as in [Serrano11]
		 * 
		 * Read-only transaction does not propagate
		 */
		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION
				|| !isWCoordinator(executionHistory))
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

		ParallelSnapshotIsolationPiggyback pb = receivedPiggybacks
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
	public void postAbort(ExecutionHistory executionHistory, Vote vote){
		
		
		if (!isWCoordinator(executionHistory))
			return;
		
		ParallelSnapshotIsolationPiggyback pb = (ParallelSnapshotIsolationPiggyback) vote
						.getVotePiggyBack().getPiggyback();
		
		Set<String> dest = new HashSet<String>();
		for (Group group : manager.getReplicaGroups()){
			if (!manager.getMyGroup().name().equals(group))
				dest.add(group.name());
		}
		
		ParallelSnapshotIsolationPropagateMessage msg = new ParallelSnapshotIsolationPropagateMessage(
				pb, dest, manager.getMyGroup().name(),
				manager.getSourceId());
		/*
		 * Send to every other groups except myself
		 */
		propagation.propagate(msg);
		

		/*
		 * send to myself
		 */
		learn(null, msg);
		
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

	/**
	 * @inheritDoc
	 */
	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory,
			ConcernedKeysTarget target) {
		Set<String> keys = new HashSet<String>();
		keys.addAll(executionHistory.getWriteSet().getKeys());
		keys.addAll(executionHistory.getCreateSet().getKeys());
		return keys;
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public TerminationCommunication getOrCreateTerminationCommunication(
			Group group, Learner learner) {
		if (terminationCommunication == null)
			terminationCommunication = new GenuineTerminationCommunication(
					group, learner);
		return terminationCommunication;
	}

	private static AtomicInteger tempSequenceNumber=new AtomicInteger(0);

	@Override
	public void transactionDeliveredForTermination(TerminateTransactionRequestMessage msg){
		if (isWCoordinator(msg.getExecutionHistory())) {
			int sequenceNumber=0;
			
			if (msg.getExecutionHistory().getTransactionType()!=TransactionType.INIT_TRANSACTION)
				sequenceNumber=tempSequenceNumber.incrementAndGet();
			
			msg.setComputedObjectUponDelivery(new Integer(sequenceNumber));
		}
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
		if (isWCoordinator(executionHistory)) {

			int sequenceNumber=(Integer)object;

			vp = new VotePiggyback(new ParallelSnapshotIsolationPiggyback(
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
	private boolean isWCoordinator(ExecutionHistory executionHistory) {

		String key;
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

		return false;

	}

	/**
	 * @inheritDoc
	 */
	public void voteReceived(Vote vote) {
		if (vote.getVotePiggyBack() != null)
			receivedPiggybacks.put(vote.getTransactionHandler().getId(),
					(ParallelSnapshotIsolationPiggyback) vote
							.getVotePiggyBack().getPiggyback());
	}

}
