package fr.inria.jessy.consistency;

import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VectorFactory;

public class UpdateSerializabilityWithDependenceVector extends UpdateSerializability {

	public UpdateSerializabilityWithDependenceVector(DataStore dateStore) {
		super(dateStore);
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
				tmp.getLocalVector().increment();
			}

			logger.debug(executionHistory.getTransactionHandler() + " >> "
					+ transactionType.toString()
					+ " >> INIT_TRANSACTION COMMITTED");
			return true;
		}

		/*
		 * If the transaction is not init, we consider the create operations as
		 * update operations. Thus, we move them to the writeSet List.
		 */
		executionHistory.getWriteSet().addEntity(
				executionHistory.getCreateSet());

		JessyEntity lastComittedEntity;

		/*
		 * Firstly, the writeSet is checked.
		 */
		for (JessyEntity tmp : executionHistory.getWriteSet().getEntities()) {
			try {
				lastComittedEntity = store
						.get(new ReadRequest<JessyEntity>(
								(Class<JessyEntity>) tmp.getClass(),
								"secondaryKey", tmp.getKey(), null))
						.getEntity().iterator().next();

				if (lastComittedEntity.getLocalVector().isCompatible(
						tmp.getLocalVector()) != Vector.CompatibleResult.COMPATIBLE) {
					logger.warn("Certification fails for transaction "
							+ executionHistory.getTransactionHandler().getId()
							+ " because it has written " + tmp.getKey()
							+ " with version " + tmp.getLocalVector()
							+ " but the last committed version is : "
							+ lastComittedEntity.getLocalVector());
					return false;
				}

			} catch (NullPointerException e) {
				// nothing to do.
				// the key is simply not there.
			}

		}

		/*
		 * Secondly, the readSet is checked.
		 */
		for (JessyEntity tmp : executionHistory.getReadSet().getEntities()) {
			try {
				lastComittedEntity = store
						.get(new ReadRequest<JessyEntity>(
								(Class<JessyEntity>) tmp.getClass(),
								"secondaryKey", tmp.getKey(), null))
						.getEntity().iterator().next();

				if (lastComittedEntity.getLocalVector().isCompatible(
						tmp.getLocalVector()) != Vector.CompatibleResult.COMPATIBLE) {

					logger.warn("Certification fails for transaction "
							+ executionHistory.getTransactionHandler().getId()
							+ " because it has written " + tmp.getKey()
							+ " with version " + tmp.getLocalVector()
							+ " but the last committed version is : "
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
	 * With dependence vector, applying transactions can be done in parallel without any problem.
	 */
	@Override
	public boolean applyingTransactionCommute() {
		return true;
	}

	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		ExecutionHistory executionHistory=msg.getExecutionHistory();
		// updatedVector is a new vector. It will be used as a new
		// vector for all modified vectors.
		Vector<String> updatedVector = VectorFactory.getVector("");
		updatedVector.update(executionHistory.getReadSet().getCompactVector(),
				executionHistory.getWriteSet().getCompactVector());

		for (JessyEntity entity : executionHistory.getWriteSet().getEntities()) {
			updatedVector.setSelfKey(entity.getLocalVector().getSelfKey());
			entity.setLocalVector(updatedVector.clone());
		}

	}

}
