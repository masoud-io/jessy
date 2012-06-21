package fr.inria.jessy.consistency;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.GenuineTerminationCommunication;
import fr.inria.jessy.communication.MessagePropagation;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.communication.message.ParallelSnapshotIsolationPropagateMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionHandler;
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
 */
public class ParallelSnapshotIsalation extends Consistency implements Learner {

	private static Logger logger = Logger
			.getLogger(ParallelSnapshotIsalation.class);

	static {
		votePiggybackRequired = true;
	}

	MessagePropagation propagation;

	private ConcurrentHashMap<TransactionHandler, ParallelSnapshotIsolationPiggyback> receivedPiggybacks;

	public ParallelSnapshotIsalation(DataStore store) {
		super(store);
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

			executionHistory.getWriteSet().addEntity(
					executionHistory.getCreateSet());

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
		ExecutionHistory executionHistory = pb.executionHistory;
		/*
		 * Two conditions should be held before applying the updates. Figure 13
		 * of [Serrano2011]
		 * 
		 * 1) committedVTS should be greater or equal to startVTS (in order to
		 * ensure causality)
		 * 
		 * 2)committedVTS[group]>sequenceNumber (in order to ensure that all
		 * transactions serially applies)
		 */
		CompactVector<String> startVTS = executionHistory.getReadSet()
				.getCompactVector();
		while ((VersionVector.committedVTS.compareTo(startVTS) < 0)
				|| (VersionVector.committedVTS
						.getValue(pb.wCoordinatorGroupName) < pb.sequenceNumber - 1)) {
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

		VersionVector.committedVTS.getVector().put(pb.wCoordinatorGroupName,
				pb.sequenceNumber);
		VersionVector.committedVTS.notifyAll();

		logger.info("updateCommittedVTS set observedCommittedTransactions to "
				+ VersionVector.committedVTS.getVector().toString());
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {

		/*
		 * Trying to commit a transaction without receiving the sequence number.
		 * Something is wrong.
		 */
		if (!receivedPiggybacks.contains(executionHistory
				.getTransactionHandler())) {
			System.exit(0);
		}
		ParallelSnapshotIsolationPiggyback pb = receivedPiggybacks
				.get(executionHistory.getTransactionHandler());

		/*
		 * Wait until its conditions holds true, and then update the
		 * CommittedVTS
		 */
		updateCommittedVTS(pb);

		/*
		 * updatedVector is a new vector. It will be used as a new vector for
		 * all modified vectors.
		 * 
		 * <p> The update takes place according to Walter [Serrano2011]
		 */

		VersionVector<String> updatedVector = new VersionVector<String>(
				pb.wCoordinatorGroupName, pb.sequenceNumber);

		for (JessyEntity entity : executionHistory.getWriteSet().getEntities()) {
			entity.setLocalVector(updatedVector.clone());

			logger.info("Prepared to commit set local vector of  "
					+ entity.getKey() + " to " + updatedVector.toString());
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
				getConcerningKeys(executionHistory)));

		/*
		 * Compute the set of jessy groups that have not receive the vector.
		 * I.e., those groups that are not concerned by the transaction.
		 */
		for (Group group : manager.getReplicaGroups()) {
			if (!alreadyNotified.contains(group.name())) {
				dest.add(group.name());
			}
		}

		ParallelSnapshotIsolationPiggyback pb = new ParallelSnapshotIsolationPiggyback(
				manager.getMyGroup().name(),
				VersionVector.committedVTS
						.getValue(manager.getMyGroup().name()),
				executionHistory);

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
			updateCommittedVTS(msg.getParallelSnapshotIsolationPiggyback());

		}
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory) {
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
			System.out.println(key);
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
			receivedPiggybacks.put(vote.getTransactionHandler(),
					(ParallelSnapshotIsolationPiggyback) vote
							.getVotePiggyBack().getPiggyback());
	}

	/**
	 * Used in the {@code Vote} for sending the new sequence number to the write
	 * set of the transaction.
	 * 
	 * @author Masoud Saeida Ardekani
	 * 
	 */
	public class ParallelSnapshotIsolationPiggyback implements Externalizable {

		/**
		 * The group name of the jessy instances that replicate the first write
		 * in the transaction
		 */
		public String wCoordinatorGroupName;

		/**
		 * Increamented sequenceNumber of the jessy instance of the group
		 * {@code wCoordinatorGroupName}
		 */
		public Integer sequenceNumber;

		public ExecutionHistory executionHistory;

		@Deprecated
		public ParallelSnapshotIsolationPiggyback() {
		}

		public ParallelSnapshotIsolationPiggyback(String wCoordinatorGroupName,
				Integer sequenceNumber, ExecutionHistory executionHistory) {
			this.wCoordinatorGroupName = wCoordinatorGroupName;
			this.sequenceNumber = sequenceNumber;
			this.executionHistory = executionHistory;
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			wCoordinatorGroupName = (String) in.readObject();
			sequenceNumber = (Integer) in.readObject();
			executionHistory = (ExecutionHistory) in.readObject();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeObject(wCoordinatorGroupName);
			out.writeObject(sequenceNumber);
			out.writeObject(executionHistory);
		}

	}
}
