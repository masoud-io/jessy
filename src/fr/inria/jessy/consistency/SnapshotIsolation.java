package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.utils.CollectionUtils;

import org.apache.log4j.Logger;

import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionTouchedKeys;
import fr.inria.jessy.transaction.termination.Vote;
import fr.inria.jessy.vector.ScalarVector;
import fr.inria.jessy.vector.Vector;

public class SnapshotIsolation extends Consistency {
	private static Logger logger = Logger
			.getLogger(SnapshotIsolation.class);

	static{
		READ_KEYS_REQUIRED_FOR_COMMUTATIVITY_TEST=false;
	}
	
	public SnapshotIsolation(JessyGroupManager m, DataStore store) {
		super(m, store);
		Consistency.SEND_READSET_DURING_TERMINATION=false;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean certify(ExecutionHistory executionHistory) {

		TransactionType transactionType = executionHistory.getTransactionType();

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

			return true;
		}

		/*
		 * If the transaction is not read-only or init, we consider the create
		 * operations as update operations. Thus, we move them to the writeSet
		 * List.
		 */
		if (executionHistory.getCreateSet()!=null)
			executionHistory.getWriteSet().addEntity(executionHistory.getCreateSet());

		JessyEntity lastComittedEntity;
		for (JessyEntity tmp : executionHistory.getWriteSet().getEntities()) {

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

					logger.debug("lastCommitted: "
							+ lastComittedEntity.getLocalVector() + " tmp: "
							+ tmp.getLocalVector());

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
			return !CollectionUtils.isIntersectingWith(history1.getWriteSet()
					.getKeys(), history2.getWriteSet().getKeys());
	}
	
	@Override
	public boolean certificationCommute(TransactionTouchedKeys tk1,
			TransactionTouchedKeys tk2) {
		return false;
	}
	
	@Override
	public boolean applyingTransactionCommute() {
		return false;
	}
	
	@Override
	public void transactionDeliveredForTermination(TerminateTransactionRequestMessage msg){
		try{
			int newVersion;
			// WARNING: there is a cast to ScalarVector
			if (msg.getExecutionHistory().getTransactionType() != TransactionType.INIT_TRANSACTION) {
				newVersion = ScalarVector.incrementAndGetLastCommittedSeqNumber();
			} else {
				newVersion = 0;
			}
			
			ScalarVector.committingTransactionSeqNumber.offer(newVersion);
			msg.setComputedObjectUponDelivery(newVersion);
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
	}

	/**
	 * update (1) the {@link lastCommittedTransactionSeqNumber}: incremented by
	 * 1 (2) the {@link committedWritesets} with the new
	 * {@link lastCommittedTransactionSeqNumber} and {@link committedWritesets}
	 * (3) the scalar vector of all updated or created entities
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		ExecutionHistory executionHistory=msg.getExecutionHistory();

		int newVersion=(Integer)msg.getComputedObjectUponDelivery();		
		// WARNING: there is a cast to ScalarVector
		if (executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) {

			for (JessyEntity je : executionHistory.getWriteSet().getEntities()) {
				((ScalarVector) je.getLocalVector()).update(newVersion);
			}

		} else {
			for (JessyEntity je : executionHistory.getCreateSet().getEntities()) {
				((ScalarVector) je.getLocalVector()).update(newVersion);
			}
		}

		ScalarVector.removeCommittingTransactionSeqNumber(newVersion);
		
		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ "COMMITED, lastCommittedTransactionSeqNumber:"
				+ ScalarVector.getLastCommittedSeqNumber());
	}
	
	public void postAbort(TerminateTransactionRequestMessage msg, Vote Vote){
		ScalarVector.removeCommittingTransactionSeqNumber((Integer)msg.getComputedObjectUponDelivery());
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory,
			ConcernedKeysTarget target) {

		Set<String> keys = new HashSet<String>(4);
		if (target == ConcernedKeysTarget.TERMINATION_CAST) {

			/*
			 * If it is a read-only transaction, we return an empty set. But if
			 * it is not an empty set, then we have to return a set that
			 * contains a key every group. We do this to simulate the atomic
			 * broadcast behavior. Because, later, this transaction will atomic
			 * multicast to all the groups.
			 */
			if (executionHistory.getWriteSet().size() == 0
					&& executionHistory.getCreateSet().size() == 0)
				return new HashSet<String>(0);

			keys=manager.getPartitioner().generateKeysInAllGroups();
			return keys;
		} else if (target == ConcernedKeysTarget.SEND_VOTES) {
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
			return keys;
		} else {
			keys=manager.getPartitioner().generateKeysInAllGroups();
			return keys;
		}
	}

	@Override
	public Set<String> getVotersToCoordinator(
			Set<String> termincationRequestReceivers,
			ExecutionHistory executionHistory) {
		
		Set<String> keys = new HashSet<String>(4);
		keys.addAll(executionHistory.getWriteSet().getKeys());
		keys.addAll(executionHistory.getCreateSet().getKeys());

		Set<String> destGroups = new HashSet<String>(4);
		
		destGroups
		.addAll(manager.getPartitioner().resolveNames(keys));

		
		return destGroups;
	}
}
