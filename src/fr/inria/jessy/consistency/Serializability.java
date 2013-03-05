package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.utils.CollectionUtils;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.Vector;

public class Serializability extends Consistency {

	private static Logger logger = Logger.getLogger(Serializability.class);

	public Serializability(DataStore dateStore) {
		super(dateStore);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean certify(ExecutionHistory executionHistory) {
		TransactionType transactionType = executionHistory.getTransactionType();

		/*
		 * if the transaction is an initialization transaction, it first
		 * increments the vectors and then commits.
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
		if (executionHistory.getCreateSet()!=null)
			executionHistory.getWriteSet().addEntity(executionHistory.getCreateSet());

		JessyEntity lastComittedEntity;

		/*
		 * Firstly, the writeSet is checked.
		 */
		if (executionHistory.getWriteSet()!=null){
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
		}

		/*
		 * Secondly, the readSet is checked.
		 */
		if (executionHistory.getReadSet()!=null){
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

		}

		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ transactionType.toString() + " >> COMMITTED");
		return true;
	}

	/**
	 * TODO Consider all cases
	 */
	@Override	
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {

		boolean result=true;;
		
		if (history1.getReadSet()!=null && history2.getWriteSet()!=null){
			result = !CollectionUtils.isIntersectingWith(history2.getWriteSet()
					.getKeys(), history1.getReadSet().getKeys());
		}
		if (history1.getWriteSet()!=null && history2.getReadSet()!=null){
			result = result && !CollectionUtils.isIntersectingWith(history1.getWriteSet()
					.getKeys(), history2.getReadSet().getKeys());
		}
		
		return result;
		
//		return !CollectionUtils.isIntersectingWith(history1.getWriteSet()
//				.getKeys(), history2.getReadSet().getKeys())
//				&& !CollectionUtils.isIntersectingWith(history2.getWriteSet()
//						.getKeys(), history1.getReadSet().getKeys());

	}
	
	@Override
	public boolean applyingTransactionCommute() {
		return true;
	}

	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		
		for (JessyEntity entity : msg.getExecutionHistory().getWriteSet().getEntities()) {
			entity.getLocalVector().update(null, null);
		}
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory,
			ConcernedKeysTarget target) {
		Set<String> keys = new HashSet<String>();
		if (target == ConcernedKeysTarget.TERMINATION_CAST ) {
			keys.addAll(executionHistory.getReadSet().getKeys());
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
		}else if(target == ConcernedKeysTarget.SEND_VOTES){
			
			if (executionHistory.getReadSet()!=null)
				keys.addAll(executionHistory.getReadSet().getKeys());
			
			if (executionHistory.getWriteSet()!=null)
				keys.addAll(executionHistory.getWriteSet().getKeys());
			
			if (executionHistory.getCreateSet()!=null)
				keys.addAll(executionHistory.getCreateSet().getKeys());
		}
		else {
			if (executionHistory.getWriteSet()!=null)
				keys.addAll(executionHistory.getWriteSet().getKeys());
			
			if (executionHistory.getCreateSet()!=null)
				keys.addAll(executionHistory.getCreateSet().getKeys());
		}
		return keys;
	}

}
