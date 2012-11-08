package fr.inria.jessy.consistency;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.ExecutorPool;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.GenuineTerminationCommunication;
import fr.inria.jessy.communication.MessagePropagation;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.communication.message.ParallelSnapshotIsolationPropagateMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.termination.Vote;
import fr.inria.jessy.transaction.termination.VotePiggyback;
import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VersionVector;

/**
 * PSI implementation according to [Serrano2011] paper.
 * 
 * @author Masoud Saeida Ardekani
 * 
 *         TODO: Implementation is not fault tolerant. I.e., if an instance does
 *         not receive a sequence number, it will block!
 * 
 */
public class ParallelSnapshotIsalation extends Consistency implements Learner {

	private ExecutorPool pool = ExecutorPool.getInstance();

	private static Logger logger = Logger
			.getLogger(ParallelSnapshotIsalation.class);

	static {
		votePiggybackRequired = true;
	}

	MessagePropagation propagation;

	private ConcurrentHashMap<UUID, ParallelSnapshotIsolationPiggyback> receivedPiggybacks;

	public ParallelSnapshotIsalation(DataStore store) {
		super(store);
		receivedPiggybacks = new ConcurrentHashMap<UUID, ParallelSnapshotIsolationPiggyback>();
		propagation = new MessagePropagation(this);
	}

	/**
	 * @inheritDoc
	 * 
	 *             According to the implementatin of VersionVector, commute in
	 *             certification is not allowed.
	 *             <p>
	 *             Assume there are two servers s1 and s2 that replicate objects
	 *             x and y. Also assume there are two transaction t1 and t2 that
	 *             writes a new value on x and y accordingly. Since
	 *             commutativity may lead to execution of t1 and t2 in different
	 *             orders on s1 and s2, and version vector cannot distinguish
	 *             this re-ordering, it can lead to some strange behavior.
	 *             (i.e., reading inconsistent snapshots!)
	 */
	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {
		return false;
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public boolean certify(ExecutionHistory executionHistory) {
		TransactionType transactionType = executionHistory.getTransactionType();

		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ transactionType.toString());
		logger.debug("ReadSet Vector"
				+ executionHistory.getReadSet().getCompactVector().toString());
		logger.debug("CreateSet Vectors"
				+ executionHistory.getCreateSet().getCompactVector().toString());
		logger.debug("WriteSet Vectors"
				+ executionHistory.getWriteSet().getCompactVector().toString());

		/*
		 * if the transaction is a read-only transaction, it commits right away.
		 */
		if (transactionType == TransactionType.READONLY_TRANSACTION) {
			logger.debug(executionHistory.getTransactionHandler() + " >> "
					+ transactionType.toString() + " >> COMMITTED");
			return true;
		}

		/*
		 * if the transaction is an initalization transaction, it first
		 * increaments the vectors and then commits.
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

					logger.error("Aborting a transaction because local vector is "
							+ tmp.getLocalVector()
							+ " and last committed is "
							+ lastComittedEntity.getLocalVector());

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
			 * Get and remove the piggybacked sequence number. We do not need it
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
			updateCommittedVTS(pb);

			/*
			 * updatedVector is a new vector. It will be used as a new vector
			 * for all modified vectors.
			 * 
			 * <p> The update takes place according to Walter [Serrano2011]
			 */

			VersionVector<String> updatedVector = new VersionVector<String>(
					pb.wCoordinatorGroupName, pb.sequenceNumber);

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
						ConcernedKeysTarget.EXCHANGE_VOTES)));

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
			pool.submit(new updateCommittedVTSTask(msg
					.getParallelSnapshotIsolationPiggyback()));

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

	/**
	 * @inheritDoc
	 */
	@Override
	public Vote createCertificationVote(ExecutionHistory executionHistory) {

		boolean isAborted = executionHistory.getTransactionType() == BLIND_WRITE
				|| certify(executionHistory);

		/*
		 * Create the piggyback vote if this instance is member of a group where
		 * the first write is for.
		 */
		VotePiggyback vp = null;
		if (isWCoordinator(executionHistory)) {

			int sequenceNumber = VersionVector.committedVTS.getValue(manager
					.getMyGroup().name()) + 1;

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
	 * certifiaction.
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

	/**
	 * Waits until the its conditions hold true, and then update the
	 * committedVTS according to the received sequence number.
	 * 
	 * @param pb
	 *            Received piggyback that contains the group name and its new
	 *            sequence number
	 * @param executionHistory
	 * 
	 *            TODO waiting on committedVTS might not be SAFE. Talk with
	 *            Pierre about it!
	 */
	private void updateCommittedVTS(ParallelSnapshotIsolationPiggyback pb) {

		if (VersionVector.committedVTS.getValue(pb.wCoordinatorGroupName) > pb.sequenceNumber) {
			/*
			 * If it has been applied before, ignore this one.
			 * 
			 * In theory, this if must never occur.
			 */
			logger.error("Transaction "
					+ pb.executionHistory.getTransactionHandler().getId()
					+ " wants to update " + pb.wCoordinatorGroupName + " : "
					+ pb.sequenceNumber);
			// System.exit(0);
			return;
		}

		ExecutionHistory executionHistory = pb.executionHistory;

		/*
		 * Two conditions should be held before applying the updates. Figure 13
		 * of [Serrano2011]
		 * 
		 * 1) committedVTS should be greater or equal to startVTS (in order to
		 * ensure causality)
		 * 
		 * 2)committedVTS[group]>sequenceNumber (in order to ensure that all
		 * transactions are serially applied)
		 * 
		 * TODO: if the propagation uses reliable broadcast, the threshold
		 * condition is useless, but since the propagation is not reliable yet,
		 * upon one message lost, the whole piggybacks might ends up waiting for
		 * that lost piggyback before being applied. The above if condition
		 * ensures that the threshold bypassing is still safe because if later
		 * an old propagation is received, the execution will be terminated.
		 */
		CompactVector<String> startVTS = executionHistory.getReadSet()
				.getCompactVector();
		while ((VersionVector.committedVTS.compareTo(startVTS) < 0)
				|| ((VersionVector.committedVTS
						.getValue(pb.wCoordinatorGroupName) < pb.sequenceNumber - 1) && (pb.sequenceNumber
						- VersionVector.committedVTS
								.getValue(pb.wCoordinatorGroupName) < ConstantPool.JESSY_PSI_PROPAGATION_THRESHOLD))) {
			/*
			 * We have to wait until committedVTS becomes updated through
			 * propagation.
			 */
			synchronized (VersionVector.committedVTS) {
				try {
					VersionVector.committedVTS.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

		VersionVector.committedVTS.setVector(pb.wCoordinatorGroupName,
				pb.sequenceNumber);

		synchronized (VersionVector.committedVTS) {
			VersionVector.committedVTS.notifyAll();
		}

	}

	/**
	 * Update the committedVTS according to the received piggy backed sequence
	 * number.
	 * 
	 */
	private class updateCommittedVTSTask implements Runnable {

		private ParallelSnapshotIsolationPiggyback piggyback;

		private updateCommittedVTSTask(
				ParallelSnapshotIsolationPiggyback piggyback) {
			this.piggyback = piggyback;
		}

		@Override
		public void run() {
			updateCommittedVTS(piggyback);
		}

	}
}
