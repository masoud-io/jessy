package fr.inria.jessy.consistency;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.ValueVector.ComparisonResult;
import fr.inria.jessy.vector.Vector;
import org.apache.log4j.PropertyConfigurator;

/**
 * This class implements Non-Monotonic Snapshot Isolation consistency criterion.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class NonMonotonicSnapshotIsolation implements Consistency {

	private static Logger logger = Logger
			.getLogger(NonMonotonicSnapshotIsolation.class);

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

		logger.debug("ReadSet Vector"
				+ executionHistory.getReadSet().getVectors().toString());
		logger.debug("WriteSet Vectors"
				+ executionHistory.getWriteSet().getVectors().toString());

		boolean result;

		// if the transaction is a read-only transaction, it commits right away.
		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
			return true;

		List<? extends JessyEntity> writeSet = executionHistory.getWriteSet()
				.getEntities();

		// updatedVector is a cloned updated vector. It will be used as a new
		// vector for all modified vectors.
		Vector<String> updatedVector = writeSet.get(0).getLocalVector().clone();
		updatedVector.update(executionHistory.getReadSet().getVectors(),
				executionHistory.getWriteSet().getVectors());

		JessyEntity lastComittedEntity;

		Iterator<? extends JessyEntity> itr = writeSet.iterator();
		while (itr.hasNext()) {
			JessyEntity tmp = itr.next();

			if (lastCommittedEntities.containsKey(tmp.getKey())) {
				lastComittedEntity = lastCommittedEntities.get(tmp.getKey());

				if (!lastComittedEntity.getLocalVector().isCompatible(
						tmp.getLocalVector())) {
					return false;
				}
			}
			// set the selfkey of the updated vector and put it back in the
			// entity.
			updatedVector.setSelfKey(tmp.getLocalVector().getSelfKey());
			tmp.setLocalVector(updatedVector.clone());
			logger.debug("ResultSet Vectors"
					+ tmp.getLocalVector().toString());


		}
		result = true;

		return result;
	}
}
