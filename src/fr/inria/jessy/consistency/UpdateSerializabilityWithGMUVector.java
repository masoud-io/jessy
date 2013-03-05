package fr.inria.jessy.consistency;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import net.sourceforge.fractal.utils.CollectionUtils;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.termination.Vote;
import fr.inria.jessy.transaction.termination.VotePiggyback;
import fr.inria.jessy.vector.GMUVector;

/**
 * This class implements Update Serializability consistency criterion along with
 * using GMUVector introduced by [Peluso2012]
 * 
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class UpdateSerializabilityWithGMUVector extends UpdateSerializability {

	private static ConcurrentHashMap<UUID, GMUVector<String>> receivedVectors;

	static {
		votePiggybackRequired = true;
		receivedVectors = new ConcurrentHashMap<UUID, GMUVector<String>>();
	}

	public UpdateSerializabilityWithGMUVector(DataStore dataStore) {
		super(dataStore);
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
					if (ConstantPool.logging)
						logger.debug("Certification fails (ReadSet) : Reads key "	+ tmp.getKey() + " with the vector "
							+ tmp.getLocalVector() + " while the last committed vector is "	+ lastComittedEntity.getLocalVector());
					return false;
				}

			} catch (NullPointerException e) {
				// nothing to do.
				// the key is simply not there.
			}

		}

		for (JessyEntity tmp : executionHistory.getWriteSet().getEntities()) {

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
					if (ConstantPool.logging)
						logger.debug("Certification fails (writeSet) : Reads key "	+ tmp.getKey() + " with the vector "
							+ tmp.getLocalVector() + " while the last committed vector is "	+ lastComittedEntity.getLocalVector());
					return false;
				}

			} catch (NullPointerException e) {
				// nothing to do.
				// the key is simply not there.
			}

		}

		return true;
	}

	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {

			boolean result=true;
			if (history1.getReadSet()!=null && history2.getWriteSet()!=null){
				result = !CollectionUtils.isIntersectingWith(history2.getWriteSet()
						.getKeys(), history1.getReadSet().getKeys());
			}
			if (history1.getWriteSet()!=null && history2.getReadSet()!=null){
				result = result && !CollectionUtils.isIntersectingWith(history1.getWriteSet()
						.getKeys(), history2.getReadSet().getKeys());
			}
			return result;

	}
	
	/**
	 * With GMUVector, it is not safe to apply transactions concurrently, and they should be applied as they are delivered. 
	 */
	@Override
	public boolean applyingTransactionCommute() {
		return false;
	}

	@Override
	public void transactionDeliveredForTermination(TerminateTransactionRequestMessage msg){
		try{
			if (msg.getExecutionHistory().getTransactionType() != TransactionType.INIT_TRANSACTION) {
				GMUVector<String> prepVC = GMUVector.mostRecentVC.clone();
				int prepVCAti = GMUVector.lastPrepSC.incrementAndGet();
				prepVC.setValue(prepVC.getSelfKey(), prepVCAti);
				
				msg.setComputedObjectUponDelivery(prepVC);
			}
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
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

			GMUVector<String> prepVC = null;
			if (isCommitted
					&& executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) {
				/*
				 * We have to update the vector here, and send it over to the
				 * others. Corresponds to line 20-22 of Algorithm 4
				 */
				prepVC = (GMUVector<String>) object;
			}

			/*
			 * Corresponds to line 23
			 */
			return new Vote(executionHistory.getTransactionHandler(), isCommitted,
					JessyGroupManager.getInstance().getMyGroup().name(),
					new VotePiggyback(prepVC));
		}
		catch (Exception ex){
			ex.printStackTrace();
			return null;
		}
	}

	/**
	 * @inheritDoc
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void voteReceived(Vote vote) {
		if (vote.getVotePiggyBack().getPiggyback() == null) {
			/*
			 * transaction has been aborted, thus a null piggyback is sent along
			 * with the vote. no need for extra work.
			 */
			return;
		}

		try {
			if (vote.getVotePiggyBack() != null) {

				GMUVector<String> commitVC = (GMUVector<String>) vote
						.getVotePiggyBack().getPiggyback();

				/*
				 * Corresponds to line 19
				 */
				
				GMUVector<String> receivedVector = receivedVectors.putIfAbsent(
						vote.getTransactionHandler().getId(), commitVC);
				if (receivedVector != null) {
					receivedVector.update(commitVC);
//					receivedVectors.get(vote.getTransactionHandler().getId())
//							.update(commitVC);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		ExecutionHistory executionHistory=msg.getExecutionHistory();

		GMUVector<String> commitVC = receivedVectors.get(executionHistory
				.getTransactionHandler().getId());

		if (commitVC != null) {

			/*
			 * line 19 above is not complete. Though it calculates the max of all received votes, it does not consider the vectors of read items.
			 * now we should take the max of commitVC with every thing we have read so far.
			 */
			for (JessyEntity tmp : executionHistory.getReadSet().getEntities()) {
				commitVC.update(tmp.getLocalVector());
			}
			
			/*
			 * Corresponds to line 22
			 */
			int xactVN = 0;
			for (Entry<String, Integer> entry : commitVC.getEntrySet()) {
				if (entry.getValue() > xactVN)
					xactVN = entry.getValue();
			}

			/*
			 * Corresponds to line 24
			 */
			Set<String> dest = new HashSet<String>(JessyGroupManager
					.getInstance()
					.getPartitioner()
					.resolveNames(
							getConcerningKeys(executionHistory,
									ConcernedKeysTarget.RECEIVE_VOTES)));

			/*
			 * setup commitVC
			 */
			for (String index : dest) {
				commitVC.setValue(index, xactVN);
			}

			/*
			 * Assigning commitVC to the entities
			 */
			for (JessyEntity entity : executionHistory.getWriteSet()
					.getEntities()) {
				entity.setLocalVector(commitVC.clone());
			}
		}

	}

	public void postCommit(ExecutionHistory executionHistory) {
		if (executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) {
			/*
			 * Corresponds to line 26-27
			 * 
			 * We only need a final vector for one of the written objects. Thus,
			 * we choose the first one.
			 */
			GMUVector<String> commitVC = receivedVectors.get(executionHistory
					.getTransactionHandler().getId());
			if (GMUVector.lastPrepSC.get() < commitVC.getValue(manager.getMyGroup().name())) {
				GMUVector.lastPrepSC.set(commitVC.getValue(manager.getMyGroup().name()));
			}
			
			/*
			 * Corresponds to line 31
			 */
			GMUVector.mostRecentVC=commitVC.clone();
		}
		

		/*
		 * Garbage collect the received vectors. We don't need them anymore.
		 */
		if (receivedVectors.containsKey(executionHistory
				.getTransactionHandler().getId()))
			receivedVectors.remove(executionHistory.getTransactionHandler()
					.getId());

	}


}
