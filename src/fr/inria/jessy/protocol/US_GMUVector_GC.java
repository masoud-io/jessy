package fr.inria.jessy.protocol;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import net.sourceforge.fractal.utils.ExecutorPool;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

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
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.termination.vote.Vote;
import fr.inria.jessy.transaction.termination.vote.VotePiggyback;
import fr.inria.jessy.transaction.termination.vote.VotingQuorum;
import fr.inria.jessy.vector.GMUVector2;

/**
 * This class implements Update Serializability consistency criterion along with
 * using GMUVector introduced by [Peluso2012]
 * 
 * CONS: US
 * Vector: GMUVector
 * Atomic Commitment: GroupCommunication
 * 
 * @author Masoud Saeida Ardekani
 * 
 * TODO Certify and GMUVector should be double checked. 
 * 
 */
public class US_GMUVector_GC extends US{

	private static ConcurrentHashMap<UUID, GMUVector2<String>> receivedVectors;
	private static ConcurrentHashMap<UUID, Integer> seqNos;
	private static ApplyGMUVector2 applyGMUVector;
	
	static {
		ConstantPool.ATOMIC_COMMIT=ATOMIC_COMMIT_TYPE.GROUP_COMMUNICATION;
		votePiggybackRequired = true;
		receivedVectors = new ConcurrentHashMap<UUID, GMUVector2<String>>();
		seqNos=new ConcurrentHashMap<UUID, Integer>();
	}

	public US_GMUVector_GC(JessyGroupManager m, DataStore dataStore) {
		super(m, dataStore);
		
		if (!m.isProxy()){
			applyGMUVector=new ApplyGMUVector2();
			ExecutorPool.getInstance().submit(applyGMUVector);
		}
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
						.getLocalVector().getValue(tmp.getKey())) {
					if (ConstantPool.logging)
						logger.error("Transaction "+ executionHistory.getTransactionHandler().getId() + "Certification fails (writeSet) : Reads key "	+ tmp.getKey() + " with the vector "
							+ tmp.getLocalVector() + " while the last committed vector is "	+ lastComittedEntity.getLocalVector());
					return false;
				}

			} catch (NullPointerException e) {
				// nothing to do.
				// the key is simply not there.
			}

		}
		
//		for (JessyEntity tmp : executionHistory.getWriteSet().getEntities()) {
//			if (!manager.getPartitioner().isLocal(tmp.getKey()))
//				continue;
//
//			try {
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
//						.getLocalVector().getValue(tmp.getKey())) {
//					if (ConstantPool.logging)
//						logger.error("Transaction "+ executionHistory.getTransactionHandler().getId() + "Certification fails (writeSet) : Reads key "	+ tmp.getKey() + " with the vector "
//							+ tmp.getLocalVector() + " while the last committed vector is "	+ lastComittedEntity.getLocalVector());
//					return false;
//				}
//				
//			} catch (NullPointerException e) {
////				e.printStackTrace();
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
		return true;
	}

	@Override
	public boolean transactionDeliveredForTermination(ConcurrentLinkedHashMap<UUID, Object> terminatedTransactions, ConcurrentHashMap<TransactionHandler, VotingQuorum>  quorumes, TerminateTransactionRequestMessage msg){
		try{
			if (msg.getExecutionHistory().getTransactionType() != TransactionType.INIT_TRANSACTION) {
				Set<String> voteReceivers =	manager.getPartitioner().resolveNames(getConcerningKeys(
								msg.getExecutionHistory(),
								ConcernedKeysTarget.RECEIVE_VOTES));
				
				if (voteReceivers.contains(manager.getMyGroup().name())){
					int prepVCAti = GMUVector2.lastPrepSC.incrementAndGet();
					msg.setComputedObjectUponDelivery((Integer)prepVCAti);
					seqNos.put(msg.getExecutionHistory().getTransactionHandler().getId(), prepVCAti);
				}
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
		try{
			/*
			 * First, it needs to run the certification test on the received
			 * execution history. A blind write always succeeds.
			 */

			boolean isCommitted = executionHistory.getTransactionType() == BLIND_WRITE
					|| certify(executionHistory);

			Integer prepVCAti = null;
			if (isCommitted
					&& executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) {
				/*
				 * We have to update the vector here, and send it over to the
				 * others. Corresponds to line 20-22 of Algorithm 4
				 */
				if (object!=null)
					prepVCAti = (Integer) object;
			}

			/*
			 * Corresponds to line 23
			 */
			return new Vote(executionHistory.getTransactionHandler(), isCommitted,
					manager.getMyGroup().name(),
					new VotePiggyback(prepVCAti));
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
			return;
		}

		try {
			Integer prepVCAti = (Integer) vote
					.getVotePiggyBack().getPiggyback();

			if (vote.getVotePiggyBack() != null && prepVCAti!=null) {

				/*
				 * Corresponds to line 19
				 */
				GMUVector2<String> receivedVector = receivedVectors.putIfAbsent(
						vote.getTransactionHandler().getId(), new GMUVector2<String>(manager.getMyGroup().name(), 0));
				if (receivedVector != null) {
					receivedVector.setValue(vote.getVoterEntityName(), prepVCAti);
				}
				else{
					receivedVectors
						.get(vote.getTransactionHandler().getId())
						.setValue(vote.getVoterEntityName(), prepVCAti);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		GMUVector2<String> commitVC = new GMUVector2<String>(manager.getMyGroup().name(), 0);
		
		try{
			ExecutionHistory executionHistory=msg.getExecutionHistory();


			for (JessyEntity tmp : executionHistory.getReadSet().getEntities()) {			
				commitVC.update(tmp.getLocalVector());
			}

			GMUVector2<String> receivedVC = receivedVectors.get(msg.getExecutionHistory().getTransactionHandler().getId());
			commitVC.update(receivedVC);

			/*
			 * Assigning commitVC to the entities
			 */
			for (JessyEntity entity : executionHistory.getWriteSet()
					.getEntities()) {
				entity.getLocalVector().getMap().clear();
				entity.getLocalVector().setValue(manager.getMyGroup().name(), (int)commitVC.getSelfValue());
			}

		}
		catch (Exception ex){
			ex.printStackTrace();
		}
		finally{
			if (msg.getExecutionHistory().getTransactionType() != TransactionType.INIT_TRANSACTION) {			
				applyGMUVector.applyCommittedGMUVector(seqNos.remove(msg.getExecutionHistory().getTransactionHandler().getId()), commitVC);
			}
		}

	}

	@Override
	public void postCommit(ExecutionHistory executionHistory) {
		try{
			if (executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) {
				applyGMUVector.GMUVectorIsAdded();
			}

		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	@Override
	public void garbageCollect(TerminateTransactionRequestMessage msg){
		/*
		 * Garbage collect the received vectors. We don't need them anymore.
		 */
		if (receivedVectors.containsKey(msg.getExecutionHistory()
				.getTransactionHandler().getId()))
			receivedVectors.remove(msg.getExecutionHistory().getTransactionHandler()
					.getId());
	}
	
	@Override
	public void postAbort(TerminateTransactionRequestMessage msg, Vote Vote){
		try{
			receivedVectors.remove(msg.getExecutionHistory().getTransactionHandler().getId());
			applyGMUVector.applyAbortedGMUVector(seqNos.remove(msg.getExecutionHistory().getTransactionHandler().getId()));
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
}
