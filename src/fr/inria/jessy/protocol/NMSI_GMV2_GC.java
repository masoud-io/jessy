package fr.inria.jessy.protocol;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import net.sourceforge.fractal.utils.CollectionUtils;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.consistency.NMSI;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.termination.vote.Vote;
import fr.inria.jessy.transaction.termination.vote.VotePiggyback;
import fr.inria.jessy.transaction.termination.vote.VotingQuorum;
import fr.inria.jessy.vector.GMUVector2;

/**
 * This class implements Non-Monotonic Snapshot Isolation consistency criterion
 * along with using GMUVector2
 * 
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class NMSI_GMV2_GC extends NMSI {

	private static ConcurrentHashMap<UUID, GMUVector2<String>> receivedVectors;

	static {
		votePiggybackRequired = true;
		receivedVectors = new ConcurrentHashMap<UUID, GMUVector2<String>>();
	}

	public NMSI_GMV2_GC(JessyGroupManager m, DataStore dataStore) {
		super(m, dataStore);
	}

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
						logger.error("Certification fails (writeSet) : Reads key "	+ tmp.getKey() + " with the vector "
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

			return !CollectionUtils.isIntersectingWith(history1.getWriteSet()
					.getKeys(), history2.getWriteSet().getKeys());
	}
	
	@Override
	public boolean applyingTransactionCommute() {
		return false;
	}

	@Override
	public boolean transactionDeliveredForTermination(ConcurrentLinkedHashMap<UUID, Object> terminatedTransactions, ConcurrentHashMap<TransactionHandler, VotingQuorum>  quorumes, TerminateTransactionRequestMessage msg){
		try{
			if (msg.getExecutionHistory().getTransactionType() != TransactionType.INIT_TRANSACTION) {
//				GMUVector2.init(manager);
				GMUVector2<String> prepVC = GMUVector2.mostRecentVC.clone();
				int prepVCAti = GMUVector2.lastPrepSC.incrementAndGet();
				prepVC.setValue(prepVC.getSelfKey(), prepVCAti);

				msg.setComputedObjectUponDelivery(prepVC);
			}
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
		
		return true;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Vote createCertificationVote(ExecutionHistory executionHistory, Object object) {
		/*
		 * First, it needs to run the certification test on the received
		 * execution history. A blind write always succeeds.
		 */
		
		boolean isCommitted = executionHistory.getTransactionType() == BLIND_WRITE
				|| certify(executionHistory);
		
		GMUVector2<String> prepVC = null;

		try{


			if (isCommitted
					&& executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) {
				/*
				 * We have to update the vector here, and send it over to the
				 * others. Corresponds to line 20-22 of Algorithm 4
				 */
				prepVC = (GMUVector2<String>) object;
			}

		}
		catch (Exception ex){
			ex.printStackTrace();
			
		}

		/*
		 * Corresponds to line 23
		 */
		return new Vote(executionHistory.getTransactionHandler(), isCommitted,
				manager.getMyGroup().name(),
				new VotePiggyback(prepVC));
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

				GMUVector2<String> commitVC = (GMUVector2<String>) vote
						.getVotePiggyBack().getPiggyback();

				/*
				 * Corresponds to line 19
				 */
				
				GMUVector2<String> receivedVector = receivedVectors.putIfAbsent(
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

		GMUVector2<String> commitVC = receivedVectors.get(executionHistory
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
			Set<String> dest = new HashSet<String>(
					manager
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
			GMUVector2<String> commitVC = receivedVectors.get(executionHistory
					.getTransactionHandler().getId());
			if (GMUVector2.lastPrepSC.get() < commitVC.getValue(manager.getMyGroup().name())) {
				GMUVector2.lastPrepSC.set(commitVC.getValue(manager.getMyGroup().name()));
			}
			
			/*
			 * Corresponds to line 31
			 */
			GMUVector2.mostRecentVC=commitVC.clone();
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
