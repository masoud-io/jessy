package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;

public class Serializability extends Consistency {

	private static Logger logger = Logger.getLogger(Serializability.class);

	public Serializability(DataStore dateStore) {
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

				if (!lastComittedEntity.getLocalVector().isCompatible(
						tmp.getLocalVector())) {
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
	public boolean hasConflict(ExecutionHistory history1,
			ExecutionHistory history2) {

		Set<String> history1Keys = history1.getWriteSet().getKeys();
		history1Keys.addAll(history1.getReadSet().getKeys());

		Set<String> history2Keys = history2.getWriteSet().getKeys();
		history2Keys.addAll(history2.getReadSet().getKeys());

		for (String key : history1Keys) {
			if (history2Keys.contains(key)) {
				return true;
			}
		}
		return false;

	}

	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {

		for (JessyEntity entity : executionHistory.getWriteSet().getEntities()) {
			entity.getLocalVector().update(null, null);
		}
	}
	
	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory) {
		Set<String> keys = new HashSet<String>();
		keys.addAll(executionHistory.getReadSet().getKeys());
		keys.addAll(executionHistory.getWriteSet().getKeys());
		keys.addAll(executionHistory.getCreateSet().getKeys());
		return keys;
	}

}
