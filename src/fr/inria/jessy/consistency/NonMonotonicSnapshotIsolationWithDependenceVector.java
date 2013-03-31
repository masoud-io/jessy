package fr.inria.jessy.consistency;


import static fr.inria.jessy.vector.ValueVector.ComparisonResult.EQUAL_TO;
import static fr.inria.jessy.vector.ValueVector.ComparisonResult.GREATER_THAN;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.ValueVector.ComparisonResult;
import fr.inria.jessy.vector.Vector;

/**
 * This class implements Non-Monotonic Snapshot Isolation consistency criterion.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class NonMonotonicSnapshotIsolationWithDependenceVector extends NonMonotonicSnapshotIsolation {

	public NonMonotonicSnapshotIsolationWithDependenceVector(JessyGroupManager m, DataStore dataStore) {
		super(m, dataStore);
	}

	@SuppressWarnings("unchecked")
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
		 * if the transaction is an initialization transaction, it first
		 * increments the vectors and then commits.
		 */
		if (transactionType == TransactionType.INIT_TRANSACTION) {
			for (JessyEntity tmp : executionHistory.getCreateSet()
					.getEntities()) {
				/*
				 * set the self key of the created vector and put it back in the
				 * entity
				 */
				tmp.getLocalVector().increment();
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
				
				ComparisonResult r = tmp.getLocalVector().compareTo(lastComittedEntity.getLocalVector());
				if( r != GREATER_THAN && r != EQUAL_TO ){
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
	public boolean applyingTransactionCommute() {
		return true;
	}

	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		ExecutionHistory executionHistory=msg.getExecutionHistory();

		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
			return;
		
		// updatedVector is a new vector. It will be used as a new
		// vector for all modified vectors.
		Vector<String> updatedVector = vfactory.getVector("");
		updatedVector.update(executionHistory.getReadSet().getCompactVector(),
				executionHistory.getWriteSet().getCompactVector());

		for (JessyEntity entity : executionHistory.getWriteSet().getEntities()) {
			updatedVector.setSelfKey(entity.getLocalVector().getSelfKey());
			entity.setLocalVector(updatedVector.clone());
		}

	}


}
