package fr.inria.jessy.consistency;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.GenuineTerminationCommunication;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.communication.VectorPropagation;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.termination.message.VectorMessage;
import fr.inria.jessy.utils.JessyGroupManager;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VectorFactory;
import fr.inria.jessy.vector.VersionVector;

public class ParallelSnapshotIsalation extends Consistency implements Learner {

	private static Logger logger = Logger
			.getLogger(ParallelSnapshotIsalation.class);

	VectorPropagation propagation;

	public ParallelSnapshotIsalation(DataStore store) {
		super(store);
		propagation = new VectorPropagation(this);
	}

	/**
	 * According to the implementatin of VersionVector, commute in certification
	 * is not allowed.
	 * <p>
	 * Assume there are two servers s1 and s2 that replicate objects x and y.
	 * Also assume there are two transaction t1 and t2 that writes a new value
	 * on x and y accordingly. Since commutativity may lead to execution of t1
	 * and t2 in different orders on s1 and s2, and version vector cannot
	 * distinguish this re-ordering, it can lead to some strange behavior.
	 * (i.e., reading inconsistent snapshots!)
	 */
	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {
		return false;
	}

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
			for (JessyEntity tmp : executionHistory.getCreateSet()
					.getEntities()) {
				/*
				 * set the selfkey of the created vector and put it back in the
				 * entity
				 */
				tmp.getLocalVector().increament();
			}

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

				if (!lastComittedEntity.getLocalVector().isCompatible(
						tmp.getLocalVector())) {
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

	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {
		// updatedVector is a new vector. It will be used as a new
		// vector for all modified vectors.
		Vector<String> updatedVector = VectorFactory.getVector("");
		updatedVector.update(executionHistory.getReadSet().getCompactVector(),
				executionHistory.getWriteSet().getCompactVector());

		VersionVector.observedCommittedTransactions = (VersionVector<String>) updatedVector
				.clone();

		for (JessyEntity entity : executionHistory.getWriteSet().getEntities()) {
			updatedVector.setSelfKey(entity.getLocalVector().getSelfKey());
			entity.setLocalVector(updatedVector.clone());
		}

	}

	@Override
	public void postCommit(ExecutionHistory executionHistory) {
		Set<String> alreadyNotified = new HashSet<String>();
		Set<String> dest = new HashSet<String>();

		alreadyNotified.addAll(JessyGroupManager.getInstance().getPartitioner()
				.resolveNames(getConcerningKeys(executionHistory)));

		/*
		 * Compute the set of jessy groups that will not receive the vector.
		 * I.e., those groups that are not concerned by the transaction.
		 */
		for (Group group : JessyGroupManager.getInstance().getReplicaGroups()) {
			if (!alreadyNotified.contains(group.name())) {
				dest.add(group.name());
			}
		}

		VectorMessage msg = new VectorMessage(
				VersionVector.observedCommittedTransactions, dest,
				JessyGroupManager.getInstance().getMyGroup().name(),
				JessyGroupManager.getInstance().getSourceId());

		propagation.propagate(msg);
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory) {
		Set<String> keys = new HashSet<String>();
		keys.addAll(executionHistory.getWriteSet().getKeys());
		keys.addAll(executionHistory.getCreateSet().getKeys());
		return keys;
	}

	@Override
	public TerminationCommunication getOrCreateTerminationCommunication(
			Group group, Group all, Collection<Group> replicaGroups,
			Learner learner) {
		if (terminationCommunication == null)
			terminationCommunication = new GenuineTerminationCommunication(
					group, all, learner);
		return terminationCommunication;
	}

	/**
	 * Receiving VersionVectors from different jessy instances.
	 * <p>
	 * upon receiving a Vector, update the VersionVector associated with each
	 * jessy instance with the received vector.
	 */
	@Override
	public void learn(Stream s, Serializable v) {
		if (v instanceof VectorMessage) {
			VersionVector<String> updatedVector = (VersionVector) v;
			VersionVector.observedCommittedTransactions.update(updatedVector);
		}
	}

}
