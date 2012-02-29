package fr.inria.jessy.consistency;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import fr.inria.jessy.store.JessyEntity;
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
public class NonMonotonicSnapshotIsolation implements Consistency {

	// TODO check if the transaction has read before write or not!!!
	/**
	 * This method performs two main tasks. First, it updates the local vector
	 * of all update entities to a new vector. Second it checks whether this
	 * transaction can commit or not.
	 */
	@Override
	public boolean certify(
			ConcurrentMap<String, JessyEntity> lastCommittedEntities,
			ExecutionHistory executionHistory) {

		// if the transaction is a read-only transaction, it commits right away.
		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
			return true;

		List<? extends JessyEntity> writeSet = executionHistory.getWriteSet();

		// updatedVector is a cloned updated vector. It will be used as a new
		// vector for all modified vectors.
		Vector<String> updatedVector = writeSet.get(0).getLocalVector().clone();
		updatedVector.update(executionHistory.getReadSetVectors(),
				executionHistory.getWriteSetVectors());

		JessyEntity lastComittedEntity;

		Iterator<? extends JessyEntity> itr = writeSet.iterator();
		while (itr.hasNext()) {
			JessyEntity tmp = itr.next();

			if (lastCommittedEntities.containsKey(tmp.getKey())) {
				lastComittedEntity = lastCommittedEntities.get(tmp.getKey());
				if (lastComittedEntity.getLocalVector()
						.compareTo(updatedVector) == ComparisonResult.GREATER_THAN) {
					return false;
				}
			} else {
				// set the selfkey of the updated vector and put it back in the
				// entity.
				updatedVector.setSelfKey(tmp.getLocalVector().getSelfKey());
				tmp.setLocalVector(updatedVector);
			}

		}
		return true;
	}
}
