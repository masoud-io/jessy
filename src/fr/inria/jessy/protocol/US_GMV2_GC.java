package fr.inria.jessy.protocol;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.ATOMIC_COMMIT_TYPE;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.consistency.US;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.termination.vote.Vote;
import fr.inria.jessy.vector.GMUVector2;

/**
 * This class implements Update Serializability consistency criterion along with
 * Partitioned Vector
 * 
 * CONS: US
 * Vector: DependenceVector
 * Atomic Commitment: GroupCommunication
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class US_GMV2_GC extends US {

	static {
		ConstantPool.ATOMIC_COMMIT=ATOMIC_COMMIT_TYPE.GROUP_COMMUNICATION;
		votePiggybackRequired = false;
	}

	public US_GMV2_GC(JessyGroupManager m, DataStore dataStore) {
		super(m, dataStore);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public boolean certify(ExecutionHistory executionHistory) {
		TransactionType transactionType = executionHistory.getTransactionType();

		if (ConstantPool.logging){
			logger.debug(executionHistory.getTransactionHandler() + " >> "
					+ transactionType.toString());
			logger.debug("ReadSet Vector"
					+ executionHistory.getReadSet().getCompactVector().toString());
			logger.debug("CreateSet Vectors"
					+ executionHistory.getCreateSet().getCompactVector().toString());
			logger.debug("WriteSet Vectors"
					+ executionHistory.getWriteSet().getCompactVector().toString());
		}

		/*
		 * if the transaction is a read-only transaction, it commits right away.
		 */
		if (transactionType == TransactionType.READONLY_TRANSACTION) {
			return true;
		}

		/*
		 * if the transaction is an initialization transaction, it first
		 * increments the vectors and then commits.
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
		for (JessyEntity tmp : executionHistory.getReadSet().getEntities()) {

			if (!manager.getPartitioner().isLocal(tmp.getKey()))
				continue;

			try {

				lastComittedEntity = store
						.get(new ReadRequest<JessyEntity>(
								(Class<JessyEntity>) tmp.getClass(),
								"secondaryKey", tmp.getKey(), null))
						.getEntity().iterator().next();

				/*
				 * instead of locking, we simply checks against the latest
				 * committed values
				 */
				if (lastComittedEntity.getLocalVector().getSelfValue() > tmp
						.getLocalVector().getSelfValue()) {
//					if (ConstantPool.logging)
						logger.error("Transaction " + executionHistory.getTransactionHandler().getId() +"Certification fails (ReadSet) : Reads key "	+ tmp.getKey() + " with the vector "
							+ tmp.getLocalVector() + " while the last committed vector is "	+ lastComittedEntity.getLocalVector());
					return false;
				}

			} catch (NullPointerException e) {
				// nothing to do.
				// the key is simply not there.
			}

		}

//		for (JessyEntity tmp : executionHistory.getWriteSet().getEntities()) {
//
//			if (!manager.getPartitioner().isLocal(tmp.getKey()))
//				continue;
//
//			try {
//
//				lastComittedEntity = store
//						.get(new ReadRequest<JessyEntity>(
//								(Class<JessyEntity>) tmp.getClass(),
//								"secondaryKey", tmp.getKey(), null))
//						.getEntity().iterator().next();
//
//				/*
//				 * instead of locking, we simply checks against the latest
//				 * committed values
//				 */
//				if (lastComittedEntity.getLocalVector().getSelfValue() > tmp
//						.getLocalVector().getSelfValue()) {
////					if (ConstantPool.logging)
//						logger.error("Transaction " + executionHistory.getTransactionHandler().getId() + " Certification fails (writeSet) : Reads key "	+ tmp.getKey() + " with the vector "
//							+ tmp.getLocalVector() + " while the last committed vector is "	+ lastComittedEntity.getLocalVector());
//					return false;
//				}
//
//			} catch (NullPointerException e) {
//				// nothing to do.
//				// the key is simply not there.
//			}
//
//		}

		return true;
	}

	/**
	 * With GMUVector, it is not safe to apply transactions concurrently, and they should be applied as they are delivered. 
	 */
	@Override
	public boolean applyingTransactionCommute() {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Vote createCertificationVote(ExecutionHistory executionHistory, Object object) {
		try{
			/*
			 * First, it needs to run the certification test on the received
			 * execution history. A blind write always succeeds.
			 */

			boolean isCommitted = executionHistory.getTransactionType() == BLIND_WRITE
					|| certify(executionHistory);

			return new Vote(executionHistory.getTransactionHandler(), isCommitted,
					manager.getMyGroup().name(),null);
		}
		catch (Exception ex){
			ex.printStackTrace();
			return null;
		}
	}


	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		ExecutionHistory executionHistory=msg.getExecutionHistory();

		GMUVector2<String> commitVC = new GMUVector2<String>(manager.getMyGroup().name(),0);


			for (JessyEntity tmp : executionHistory.getReadSet().getEntities()) {
				commitVC.update(tmp.getLocalVector());
			}
			
			int xactVN = 0;
			for (Entry<String, Integer> entry : commitVC.getEntrySet()) {
				if (entry.getValue() > xactVN)
					xactVN = entry.getValue();
			}
			xactVN++;

			Set<String> dest = new HashSet<String>(
					manager
					.getPartitioner()
					.resolveNames(
							getConcerningKeys(executionHistory,
									ConcernedKeysTarget.RECEIVE_VOTES)));

			for (String index : dest) {
				commitVC.setValue(index, xactVN);
			}

			/*
			 * Assigning commitVC to the entities
			 */
			for (JessyEntity entity : executionHistory.getWriteSet()
					.getEntities()) {
				entity.setLocalVector(commitVC.clone());
				System.out.println("Transaction " + msg.getExecutionHistory().getTransactionHandler().getId() + "Prepare to commit " + entity.getKey() + " with vector " + entity.getLocalVector() + "Readset is " + msg.getExecutionHistory().getReadSet());
			}

	}
	
}
