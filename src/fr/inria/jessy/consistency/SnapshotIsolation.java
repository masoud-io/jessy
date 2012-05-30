package fr.inria.jessy.consistency;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.NonGenuineTerminationCommunication;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.ScalarVector;

//TODO COMMENT ME
public class SnapshotIsolation extends Consistency {

	private static Logger logger = Logger
			.getLogger(SnapshotIsolation.class);
	
	public SnapshotIsolation(DataStore store) {
		super(store);
	}

	@Override
	public boolean certify(ExecutionHistory executionHistory) {

		TransactionType transactionType = executionHistory.getTransactionType();
		
		// logger.debug(executionHistory.getTransactionHandler() +
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
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {

		Set<String> history2Keys = history2.getWriteSet().getKeys();

		for (String key : history1.getWriteSet().getKeys()) {
			if (history2Keys.contains(key)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * update (1) the {@link lastCommittedTransactionSeqNumber}: incremented by
	 * 1 (2) the {@link committedWritesets} with the new
	 * {@link lastCommittedTransactionSeqNumber} and {@link committedWritesets}
	 * (3) the scalar vector of all updated or created entities
	 */
	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {
		
		if (executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) 
			ScalarVector.lastCommittedTransactionSeqNumber.incrementAndGet();

		for (JessyEntity je : executionHistory.getWriteSet().getEntities()) {
			je.getLocalVector().update(null, null);
		}

		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ "COMMITED, lastCommittedTransactionSeqNumber:"
				+ ScalarVector.lastCommittedTransactionSeqNumber.get());
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
			Group group, Group all, Collection<Group> replicaGroups, Learner learner){
		if (terminationCommunication == null){
			terminationCommunication = new NonGenuineTerminationCommunication(
					group,all, replicaGroups.iterator().next(),learner);
		}
		return terminationCommunication;
	}

}
