package fr.inria.jessy.protocol;

import java.util.Set;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.ATOMIC_COMMIT_TYPE;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.consistency.SER;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.termination.TwoPhaseCommit;
import fr.inria.jessy.vector.Vector;

/**
 * Implements a variation of P-Store [Schiper2010]
 * 
 * Instead of using atomic multicast, it uses 2PC.
 * 
 * CONS: SER
 * Vector: Light Scalar Vector
 * Atomic Commitment: Two phase commit
 * 
 * @author Masoud Saeida Ardekani
 *
 */
public class PStore_LSV_2PC extends SER {

	private static Logger logger = Logger.getLogger(PStore_LSV_2PC.class);
	
	static {
		ConstantPool.PROTOCOL_ATOMIC_COMMIT=ATOMIC_COMMIT_TYPE.TWO_PHASE_COMMIT;
	}

	public PStore_LSV_2PC(JessyGroupManager m, DataStore dateStore) {
		super(m, dateStore);
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

			if (ConstantPool.logging)
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
		 * Secondly, the readSet is checked.
		 */
		if (executionHistory.getReadSet()!=null){
			for (JessyEntity tmp : executionHistory.getReadSet().getEntities()) {


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

						if (ConstantPool.logging)
							logger.error("Certification fails for transaction "
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

		if (ConstantPool.logging)
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
		if (msg.getExecutionHistory().getWriteSet()==null)
			return;
		
		for (JessyEntity entity : msg.getExecutionHistory().getWriteSet().getEntities()) {
			entity.getLocalVector().update(null, null);
		}
	}
	
	/**
	 * Coordinator needs to only wait for the vote from the 2PC manager. 
	 * 	
	 */
	public Set<String> getVotersToJessyProxy(
			Set<String> termincationRequestReceivers,
			ExecutionHistory executionHistory) {
		termincationRequestReceivers.clear();
		termincationRequestReceivers.add(TwoPhaseCommit.getCoordinatorId(executionHistory,manager.getPartitioner()));
		return termincationRequestReceivers;
	}
}
