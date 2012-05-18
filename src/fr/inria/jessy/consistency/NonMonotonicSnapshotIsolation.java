package fr.inria.jessy.consistency;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VectorFactory;

/**
 * This class implements Non-Monotonic Snapshot Isolation consistency criterion.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class NonMonotonicSnapshotIsolation implements Consistency {

	private static Logger logger = Logger
			.getLogger(NonMonotonicSnapshotIsolation.class);

	private DataStore store;
	
	public NonMonotonicSnapshotIsolation(DataStore s) {
		this.store = s;
	}

	/**
	 * This method performs two main tasks. First, it updates the local vector
	 * of all update entities to a new vector. Second it checks whether this
	 * transaction can commit or not.
	 */
	@Override
	public boolean certify(
			ExecutionHistory executionHistory) {

		JessyEntity lastComittedEntity;

		TransactionType transactionType = executionHistory.getTransactionType();

		try{		
			logger.debug(executionHistory.getTransactionHandler() + " >> "+ transactionType.toString());
//			logger.debug("ReadSet Vector"
//					+ executionHistory.getReadSet().getCompactVector().toString());
//			logger.debug("WriteSet Vectors"
//					+ executionHistory.getWriteSet().getCompactVector().toString());
		}
		catch(Exception ex){
			ex.printStackTrace();
			
		}
		

		/*
		 * if the transaction is a read-only transaction, it commits right away.
		 */
		if (transactionType == TransactionType.READONLY_TRANSACTION) {			
			logger.debug(executionHistory.getTransactionHandler() + " >> "+ transactionType.toString() + " >> COMMITTED");
			return true;
		}

		/*
		 * if the transaction is an initalization transaction, it first
		 * increaments the vectors and then commits.
		 */
		if (transactionType == TransactionType.INIT_TRANSACTION) {
			Collection<? extends JessyEntity> createSet = executionHistory
					.getCreateSet().getEntities();
			Iterator<? extends JessyEntity> itr = createSet.iterator();
			while (itr.hasNext()) {
				JessyEntity tmp = itr.next();

				// set the selfkey of the created vector and put it back in the
				// entity.
				tmp.getLocalVector().increament();
			}
			logger.debug(executionHistory.getTransactionHandler() + " >> "+ transactionType.toString() + " >> INIT_TRANSACTION COMMITTED");
			return true;
		}

		/*
		 * If the transaction is not read-only or init, we consider the create
		 * operations as update operations. Thus, we move them to the writeSet
		 * List.
		 */
		executionHistory.getWriteSet().addEntity(
				executionHistory.getCreateSet());

		Collection<? extends JessyEntity> writeSet = executionHistory.getWriteSet()
				.getEntities();

		Iterator<? extends JessyEntity> itr = writeSet.iterator();
		
		while (itr.hasNext()) {
			
			JessyEntity tmp = itr.next();
					
			try{
				
				lastComittedEntity = store.get(
						new ReadRequest<JessyEntity>(
								(Class<JessyEntity>)tmp.getClass(),
								"secondaryKey",tmp.getKey(),
								null)).getEntity().iterator().next();
				
				if (!lastComittedEntity.getLocalVector().isCompatible(tmp.getLocalVector())) {
						return false;
				}

			}catch(NullPointerException e){
				// nothing to do.
				// the key is simply not there.
			}
			

		}		
		logger.debug(executionHistory.getTransactionHandler() + " >> "+ transactionType.toString() + " >> COMMITTED");
		return true;
	}

	@Override
	public boolean hasConflict(ExecutionHistory history1,
			ExecutionHistory history2) {

		Set<String> history2Keys=history2.getWriteSet().getKeys();
		
		for (String key : history1.getWriteSet().getKeys()) {
			if (history2Keys.contains(key)){
				return true;
			}
		}
		return false;
	}

	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {

		// updatedVector is a new vector. It will be used as a new
		// vector for all modified vectors.
		Vector<String> updatedVector = VectorFactory.getVector("");
		updatedVector.update(executionHistory.getReadSet().getCompactVector(),
				executionHistory.getWriteSet().getCompactVector());

		for(JessyEntity entity: executionHistory.getWriteSet().getEntities()){
			entity.setLocalVector(updatedVector);
		}
		
	}
}
