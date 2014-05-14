package fr.inria.jessy.protocol;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.ATOMIC_COMMIT_TYPE;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.consistency.US;
import fr.inria.jessy.consistency.Consistency.ConcernedKeysTarget;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.TwoPhaseCommit;
import fr.inria.jessy.transaction.termination.vote.Vote;
import fr.inria.jessy.transaction.termination.vote.VotePiggyback;
import fr.inria.jessy.vector.GMUVector;

/**
 * This class implements EXACTLY [Peluso2012]: I.e., Update Serializability consistency criterion along with
 * using GMUVector introduced by 
 * 
 * CONS: US
 * Vector: GMUVector
 * Atomic Commitment: Two phase commit
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class GMU_GMV_2PC extends US{

	private static ConcurrentHashMap<UUID, GMUVector<String>> receivedVectors;
//	private static ConcurrentLinkedQueue<UUID> commitQueue;
	
	static {
		ConstantPool.PROTOCOL_ATOMIC_COMMIT=ATOMIC_COMMIT_TYPE.TWO_PHASE_COMMIT;
		receivedVectors = new ConcurrentHashMap<UUID, GMUVector<String>>();
//		commitQueue=new ConcurrentLinkedQueue<UUID>();
		votePiggybackRequired = true;
	}

	public GMU_GMV_2PC(JessyGroupManager m, DataStore dataStore) {
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
				if (lastComittedEntity.getLocalVector().getValue(""+manager.getSourceId()) > tmp
						.getLocalVector().getValue(""+manager.getSourceId())) {
					if (ConstantPool.logging)
						logger.error("Transaction "+ executionHistory.getTransactionHandler().getId() + "Certification fails (readSet) : Reads key "	+ tmp.getKey() + " with the vector "
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

	/**
	 * Since there is no concurrent conflicting transaction, it is safe to apply transactions concurrently.
	 */
	@Override
	public boolean applyingTransactionCommute() {
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

			GMUVector<String> vector = null;
			if (isCommitted
					&& executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) {
				/*
				 * We have to update the vector here, and send it over to the
				 * others. Corresponds to line 20-22 of Algorithm 4
				 */
				if (GMUVector.logCommitVC.size()>0)
					vector=GMUVector.logCommitVC.peekFirst().clone();
				else vector=new GMUVector<String>(""+manager.getSourceId(), 0);
				vector.setValue(""+manager.getSourceId(), GMUVector.lastPrepSC.incrementAndGet());
				//TODO FIX ME, not safe.
				//Transactions should be added exactly in order.
//				commitQueue.add(executionHistory.getTransactionHandler().getId());
			}

			/*
			 * Corresponds to line 23
			 */
			Vote vote= new Vote(executionHistory.getTransactionHandler(), isCommitted,
					""+manager.getSourceId(),
					new VotePiggyback(vector));
			return vote;
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
		if (vote.getVotePiggyBack()==null || vote.getVotePiggyBack().getPiggyback() == null) {
			return;
		}

		/*
		 *Only the coordinator should be here.  
		 */
		try {
			GMUVector vector = (GMUVector) vote
					.getVotePiggyBack().getPiggyback();

			if (vote.getVotePiggyBack() != null && vector!=null) {

				/*
				 * Corresponds to line 19 of algorithm 3
				 */
				GMUVector<String> receivedVector = receivedVectors.putIfAbsent(
						vote.getTransactionHandler().getId(), new GMUVector<String>(""+manager.getSourceId(), 0));
				if (receivedVector != null) {
					receivedVector.update(vector);
				}
				else{
					receivedVectors
						.get(vote.getTransactionHandler().getId()).update(vector);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	private boolean isCoordinator(TerminateTransactionRequestMessage msg){
		String firstWriteKey="";
		
		if (msg.getExecutionHistory().getWriteSet() !=null && msg.getExecutionHistory().getWriteSet().size()>0)
			firstWriteKey=msg.getExecutionHistory().getWriteSet().getKeys().iterator().next();
		else if (msg.getExecutionHistory().getCreateSet() !=null && msg.getExecutionHistory().getCreateSet().size()>0)
			firstWriteKey=msg.getExecutionHistory().getCreateSet().getKeys().iterator().next();
		
		if (manager.getPartitioner().resolve(firstWriteKey).leader() == manager.getSourceId()){
			return true;
		}
		else{
			return false;
		}
	}
	
	@Override
	public void quorumReached(TerminateTransactionRequestMessage msg,TransactionState state, Vote vote){
		if (msg.getExecutionHistory().getTransactionType()==TransactionType.INIT_TRANSACTION)
			return;
		
		if (isCoordinator(msg) && state==TransactionState.COMMITTED){

			GMUVector<String> commitVC = new GMUVector<String>(""+manager.getSourceId(), 0);

			try{
				ExecutionHistory executionHistory=msg.getExecutionHistory();


				for (JessyEntity tmp : executionHistory.getReadSet().getEntities()) {			
					commitVC.update(tmp.getLocalVector());
				}

				GMUVector<String> receivedVC = receivedVectors.get(msg.getExecutionHistory().getTransactionHandler().getId());
				commitVC.update(receivedVC);
				int max=0;
				for (Integer val:commitVC.getMap().values())
					if (val>max) max=val;

				/*
				 * Assign the max value to indexes of all processes
				 * 
				 * Line 23 and 24 of Algorithm 3
				 */
				Set<String> voteReceivers=	manager.getPartitioner().resolveNames(getConcerningKeys(
								msg.getExecutionHistory(),
								ConcernedKeysTarget.RECEIVE_VOTES));
				for (String str:voteReceivers){
					Group g=manager.getPartitioner().resolve(str);
					for (int tmpswid:g.allNodes()){
						commitVC.setValue(""+tmpswid, max);
					}
				}

				/*
				 * Assigning commitVC to the entities
				 */
				for (JessyEntity entity : executionHistory.getWriteSet()
						.getEntities()) {
					entity.getLocalVector().getMap().clear();
					entity.setLocalVector(commitVC.clone());
				}

			}
			catch (Exception ex){
				ex.printStackTrace();
			}
		}
	}
	
	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		if (msg.getExecutionHistory().getTransactionType()==TransactionType.INIT_TRANSACTION)
			return;
		
		try{
//			while (!commitQueue.peek().equals(msg.getExecutionHistory().getTransactionHandler().getId())){
//				synchronized (commitQueue) {
//					try {
//						commitQueue.wait();
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
//			}

			//we need one vector, lets take first one.
			GMUVector<String> vector=(GMUVector<String>) msg.getExecutionHistory().getWriteSet().getEntities().iterator().next().getLocalVector();
			
			if (GMUVector.logCommitVC.size() > ConstantPool.GMUVECTOR_LOGCOMMITVC_SIZE-200)
				GMUVector.logCommitVC.removeLast();
			
			GMUVector.logCommitVC.addFirst(vector.clone());

			int updatedVal=vector.getValue(""+manager.getSourceId());
			if (GMUVector.lastPrepSC.get() < updatedVal){
				GMUVector.lastPrepSC.set(updatedVal);
			}

			/*
			 * We only need a scalar 
			 */
			for (JessyEntity entity : msg.getExecutionHistory().getWriteSet()
					.getEntities()) {
				entity.getLocalVector().getMap().clear();
				entity.getLocalVector().getMap().put(""+manager.getSourceId(),updatedVal);
			}
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
		
	}

	@Override
	public void postCommit(ExecutionHistory executionHistory) {
//		try{
//			if (executionHistory.getTransactionType() != TransactionType.INIT_TRANSACTION) {
//				commitQueue.remove(executionHistory.getTransactionHandler().getId());
//				synchronized(commitQueue){
//					commitQueue.notifyAll();
//				}
//			}
//
//		}
//		catch(Exception ex){
//			ex.printStackTrace();
//		}
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
//		try{
//			if (msg.getExecutionHistory().getTransactionType() != TransactionType.INIT_TRANSACTION) {
//				commitQueue.remove(msg.getExecutionHistory().getTransactionHandler().getId());
//				synchronized(commitQueue){
//					commitQueue.notifyAll();
//				}
//			}
//
//		}
//		catch(Exception ex){
//			ex.printStackTrace();
//		}
	}

	@Override
	public Set<String> getVotersToJessyProxy(
			Set<String> termincationRequestReceivers,
			ExecutionHistory executionHistory) {
		termincationRequestReceivers.clear();
		termincationRequestReceivers.add(TwoPhaseCommit.getCoordinatorId(executionHistory,manager.getPartitioner()));
		return termincationRequestReceivers;
		
//		Set<String> concernedKeys=new HashSet<String>();
//		concernedKeys.add(TwoPhaseCommit.getDetermisticKey(executionHistory));
//		
//		return manager.getPartitioner().resolveNames(concernedKeys);		
	}
}
