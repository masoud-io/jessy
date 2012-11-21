package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.GenuineTerminationCommunication;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.ScalarVector;
import fr.inria.jessy.vector.Vector;

public class SnapshotIsolationWithMulticast extends Consistency {

	private static Logger logger = Logger
			.getLogger(SnapshotIsolationWithMulticast.class);

	public SnapshotIsolationWithMulticast(DataStore store) {
		super(store);
	}

	/**
	 * {@inheritDoc}
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

			if (!manager.getPartitioner().isLocal(tmp.getKey()))
				continue;
			
			try {

				lastComittedEntity = store
						.get(new ReadRequest<JessyEntity>(
								(Class<JessyEntity>) tmp.getClass(),
								"secondaryKey", tmp.getKey(), null))
						.getEntity().iterator().next();

				if (lastComittedEntity.getLocalVector().isCompatible(
						tmp.getLocalVector()) != Vector.CompatibleResult.COMPATIBLE) {

					logger.debug("lastCommitted: "
							+ lastComittedEntity.getLocalVector() + " tmp: "
							+ tmp.getLocalVector());

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
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {

		return false;

		// Set<String> history2Keys = history2.getWriteSet().getKeys();
		//
		// for (String key : history1.getWriteSet().getKeys()) {
		// if (history2Keys.contains(key)) {
		// return true;
		// }
		// }
		// return false;
	}

	/**
	 * update (1) the {@link lastCommittedTransactionSeqNumber}: incremented by
	 * 1 (2) the {@link committedWritesets} with the new
	 * {@link lastCommittedTransactionSeqNumber} and {@link committedWritesets}
	 * (3) the scalar vector of all updated or created entities
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {

		int newVersion;
		// WARNING: there is a cast to ScalarVector
		if (executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) {
			newVersion = ScalarVector.incrementAndGetLastCommittedSeqNumber();

			for (JessyEntity je : executionHistory.getWriteSet().getEntities()) {
				((ScalarVector) je.getLocalVector()).update(newVersion);
			}

		} else {
			newVersion = 0;
			for (JessyEntity je : executionHistory.getCreateSet().getEntities()) {
				((ScalarVector) je.getLocalVector()).update(newVersion);
			}
		}

		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ "COMMITED, lastCommittedTransactionSeqNumber:"
				+ ScalarVector.getLastCommittedSeqNumber());
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory,
			ConcernedKeysTarget target) {

		Set<String> keys = new HashSet<String>(4);
		if (target == ConcernedKeysTarget.TERMINATION_CAST) {

			/*
			 * If it is a read-only transaction, we return an empty set. But if
			 * it is not an empty set, then we have to return a set that
			 * contains a key every group. We do this to simulate the atomic
			 * broadcast behaviour. Because, later, this transaction will atomic
			 * multicast to all the groups.
			 */
			if (executionHistory.getWriteSet().size() == 0
					&& executionHistory.getCreateSet().size() == 0)
				return new HashSet<String>(0);

			// TODO this is an ad-hoc way that only works for Modulo
			// Partitioner.
			// It add arbitrary keys such that there is one key for each replica
			// group. Thus, the transaction will atomic multicast to all replica
			// groups.

			for (int i = 0; i < manager.getReplicaGroups().size(); i++) {
				keys.add("" + i);
			}

			return keys;
		} else if (target == ConcernedKeysTarget.SEND_VOTES) {
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
			return keys;
		} else {
			// TODO this is an ad-hoc way that only works for Modulo
			// Partitioner.
			// It add arbitrary keys such that there is one key for each replica
			// group. Thus, the transaction will atomic multicast to all replica
			// groups.
			for (int i = 0; i < manager.getReplicaGroups().size(); i++) {
				keys.add("" + i);
			}
			return keys;
		}
	}

	@Override
	public TerminationCommunication getOrCreateTerminationCommunication(
			Group group, Learner learner) {
		if (terminationCommunication == null) {
			terminationCommunication = new GenuineTerminationCommunication(
					group, learner);

		}
		return terminationCommunication;
	}

	@Override
	public Set<String> getVotersToCoordinator(
			Set<String> termincationRequestReceivers,
			ExecutionHistory executionHistory) {
		Set<String> keys = new HashSet<String>();
		keys.addAll(executionHistory.getWriteSet().getKeys());
		keys.addAll(executionHistory.getCreateSet().getKeys());

		return keys;
	}

}
