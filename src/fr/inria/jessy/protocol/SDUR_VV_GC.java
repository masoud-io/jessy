package fr.inria.jessy.protocol;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.utils.CollectionUtils;

import org.apache.log4j.Logger;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.ATOMIC_COMMIT_TYPE;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.MessagePropagation;
import fr.inria.jessy.communication.message.SDURPropagateMessage;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.consistency.US;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.vote.Vote;
import fr.inria.jessy.transaction.termination.vote.VotingQuorum;
import fr.inria.jessy.vector.VersionVector;

/**
 * SDUR implementation according to [SciasciaDSN2012] paper. 
 * It uses Walter vector implementation, and differs it by extending SER and not PSI.
 * 
 * CONS: SER
 * Vector: VersionVector
 * Atomic Commitment: GroupCommunication without acyclic
 * 
 * Note that although this class implements SER, the rules are like US, hence it extends US class.
 * 
 * Note that this protocol is not able to replicate the whole group.
 * In other words, replication is only allowed inside a group. 
 * E.g., if processes of g1 replicates x, then x cannot be replicated by processes in g2.  
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class SDUR_VV_GC extends US  implements Learner{

	private static Logger logger = Logger
			.getLogger(SDUR_VV_GC.class);

	static {
		votePiggybackRequired = true;
		READ_KEYS_REQUIRED_FOR_COMMUTATIVITY_TEST=false;
		ConstantPool.PROTOCOL_ATOMIC_COMMIT=ATOMIC_COMMIT_TYPE.ATOMIC_CYLCLIC_MULTICAST;
	}

	/**
	 * It is required for the additional test in {@link this#certify(ExecutionHistory)}.
	 * 
	 * More precisely, it serves the need for execution of line 39 in Algorithm 4. 
	 */
	private static ConcurrentLinkedQueue<ExecutionHistory> pendingTransactions;
	
	private static ApplySDURPropagation applySDUR;
	
	/**
	 * Used for handling lines 39 of algorithm 4	
	 */
	private static ConcurrentHashMap<Integer, ExecutionHistory> committingTransactions;
	
	
	private static AtomicInteger SC;
	private static AtomicInteger PSC; 
	private static Integer RemovedPSC;
	
	private MessagePropagation propagation;

	public SDUR_VV_GC(JessyGroupManager m, DataStore store) {
		super(m, store);
		propagation = new MessagePropagation("SDURPropagateMessage", this,m);
		
		committingTransactions=new ConcurrentHashMap<Integer, ExecutionHistory>(ConstantPool.SDUR_COMMITTED_TRANSACTIONS_SIZE);
		pendingTransactions=new ConcurrentLinkedQueue<ExecutionHistory>();
		
		SC=new AtomicInteger(0);
		PSC=new AtomicInteger(0);
		RemovedPSC=new Integer(0);
		
		applySDUR=new ApplySDURPropagation();
		
	}

	@Override
	public boolean applyingTransactionCommute() {
		return true;
	}

	/**
	 * @inheritDoc
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean certify(ExecutionHistory executionHistory) {
		TransactionType transactionType = executionHistory.getTransactionType();

		/*
		 * if the transaction is a read-only transaction, it commits right away.
		 */
		if (transactionType == TransactionType.READONLY_TRANSACTION) {
			
			if (ConstantPool.logging)
				logger.debug(executionHistory.getTransactionHandler() + " >> "
						+ transactionType.toString() + " >> COMMITTED");
			return true;
		}

		/*
		 * if the transaction is an init transaction, it first
		 * increments the vectors and then commits.
		 */
		if (transactionType == TransactionType.INIT_TRANSACTION) {

			// executionHistory.getWriteSet().addEntity(
			// executionHistory.getCreateSet());

			if (ConstantPool.logging)
				logger.debug(executionHistory.getTransactionHandler() + " >> "
						+ transactionType.toString()
						+ " >> INIT_TRANSACTION COMMITTED");
			return true;
		}

		/*
		 * If the transaction is not read-only or init, we consider the create
		 * operations as update operations. Thus, we move them to the writeSet
		 * List.
		 */
		executionHistory.getWriteSet().addEntity(
				executionHistory.getCreateSet());

		/*
		 * Line 39 to 46 of Algorithm 4
		 */
		boolean result=true;
		ExecutionHistory pendingExecutionHistory=null;
		int txnSC=((VersionVector<String>)executionHistory.getReadSet().getCompactVector().getExtraObject()).getValue(manager.getMyGroup().name());
		for (int i = txnSC; i< PSC.get(); i++){
			pendingExecutionHistory=committingTransactions.get(i);

			if (pendingExecutionHistory!=null){

				//line 40 of algorithm 4
				if (executionHistory.getReadSet()!=null && pendingExecutionHistory.getWriteSet()!=null){
					result = CollectionUtils.isIntersectingWith(pendingExecutionHistory.getWriteSet()
							.getKeys(), executionHistory.getReadSet().getKeys());
				}
				//line 41 of algorithm 4
				if (executionHistory.getWriteSet()!=null && pendingExecutionHistory.getReadSet()!=null){
					result = result || CollectionUtils.isIntersectingWith(executionHistory.getWriteSet()
							.getKeys(), pendingExecutionHistory.getReadSet().getKeys());
				}
			}			
		}
	
		//Line 43
		PSC.incrementAndGet();
		
		//Line 44 and 45
		committingTransactions.put(PSC.get(), executionHistory);		
		
		//line 15	
		pendingTransactions.offer(executionHistory);		
		
		
		if (ConstantPool.logging)
			logger.error(executionHistory.getTransactionHandler() + " >> "
					+ transactionType.toString() + " >> " + !result);

		return !result;
	}


	@Override
	public void prepareToCommit(TerminateTransactionRequestMessage msg) {
		try{
			//line 35 of algorithm 4
			String key=manager.getMyGroup().name();
			int newSC=SC.incrementAndGet();
			VersionVector<String> vector=new VersionVector<String>();
			vector.setValue(key, newSC);
			for (JessyEntity entity : msg.getExecutionHistory().getWriteSet()
					.getEntities()) {
				entity.setLocalVector(vector.clone());
			}

			/*
			 * If this is a read replica, and does not write anything, then it does not need to 
			 * propagate any message.
			 * It just needs to return safely. 
			 */
			boolean shouldPropagate=false;
			
			//SC should be propagated to other replicas modifying the object in order to update the commitVTS
			Set<String> dest = new HashSet<String>();
			for (JessyEntity entity : msg.getExecutionHistory().getWriteSet()
					.getEntities()) {
				if (!manager.getPartitioner().isLocal(entity.getKey())){
					dest.add(manager.getPartitioner().resolve(entity.getKey()).name());
				}
				else{
					shouldPropagate=true;
				}
			}

			if (!shouldPropagate)
				return;

			if (dest.size() > 0) {
				SDURPiggyback pb=new SDURPiggyback(new Integer(newSC), msg.getExecutionHistory().getTransactionHandler(), manager.getMyGroup().name());
				SDURPropagateMessage propagateMsg = new SDURPropagateMessage(pb, dest, manager.getMyGroup().name(),
						manager.getSourceId());
				propagation.propagate(propagateMsg);
			}

			
			Set<String> dest2 = new HashSet<String>();
			for (String entry:dest)
				dest2.add(entry);
			dest2.add(manager.getMyGroup().name());
			
			applySDUR.createContainer(
					msg.getExecutionHistory().getTransactionHandler(),
					dest2, 
					new SDURPiggyback(newSC,msg.getExecutionHistory().getTransactionHandler(),manager.getMyGroup().name())
					);
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	@Override
	public void learn(Stream s, Serializable v) {
		if (v instanceof SDURPropagateMessage) {
			SDURPropagateMessage msg= (SDURPropagateMessage)v;
			applySDUR.addReceivedPiggyback(msg.getSDURPiggyback().getTransactionHandler(), msg.getSDURPiggyback());
		}
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void postCommit(ExecutionHistory executionHistory) {

		/*
		 * only the WCoordinator propagates the votes as in [Serrano11]
		 * 
		 * Read-only transaction does not propagate
		 */
		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
			return;

		removeHeadOfPendingTransactions();
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public void postAbort(TerminateTransactionRequestMessage msg, Vote Vote){
		removeHeadOfPendingTransactions();
	}
	
	private void removeHeadOfPendingTransactions(){
		//line 37 of algorithm 4
		pendingTransactions.poll();
		synchronized(pendingTransactions){
			pendingTransactions.notifyAll();
		}
	}
	
	/**
	 * certification needs to always be true because we later check there is no concurrent conflicting transactions. 
	 * This corresponds to line 14 of algorithm 4. 
	 * Once the message is delivered, it needs to be certified right away.
	 */
	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {

		return true;
		
	}
	
	@Override
	public boolean transactionDeliveredForTermination(ConcurrentLinkedHashMap<UUID, Object> terminatedTransactions, ConcurrentHashMap<TransactionHandler, VotingQuorum>  quorumes, TerminateTransactionRequestMessage msg){
		try{
			/*
			 * We first garbage collect old transactions.   
			 */
			while (committingTransactions.size()>ConstantPool.SDUR_COMMITTED_TRANSACTIONS_SIZE && RemovedPSC< PSC.get()){
				committingTransactions.remove(RemovedPSC++);				
			}
			
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
		
		return true;
	}
	
	/**
	 * line 24 of algorithm 4
	 */
	@Override
	public void quorumReached(TerminateTransactionRequestMessage msg,TransactionState state, Vote vote){
		synchronized(pendingTransactions){
			while (!pendingTransactions.peek().getTransactionHandler().getId().equals(msg.getExecutionHistory().getTransactionHandler().getId())){
				try {
					pendingTransactions.wait();
					
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}


	/**
	 * In the original algorithm, votes should also be sent to the replicas that are read from. 
	 */
	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory,
			ConcernedKeysTarget target) {
		Set<String> keys = new HashSet<String>();
		if (target == ConcernedKeysTarget.TERMINATION_CAST) {
			if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
				/*
				 * If the transaction is read-only, it is not needed to be atomic
				 * multicast by the coordinator. It simply commits since it has
				 * read a consistent snapshot.
				 */
				return keys;
			else {
				/*
				 * If it is not a read-only transaction, then the transaction
				 * should be sent to every process replicating an
				 * object read or written by the transaction.
				 * 
				 * Note: sending to only write-set is not enough.
				 */
				keys.addAll(executionHistory.getReadSet().getKeys());
				keys.addAll(executionHistory.getWriteSet().getKeys());
				keys.addAll(executionHistory.getCreateSet().getKeys());
				return keys;
			}
		} else  {
			/*
			 * (target == ConcernedKeysTarget.SEND_VOTES || target == ConcernedKeysTarget.RECEIVE_VOTES)
			 * Since the transaction is sent to all jessy instances replicating
			 * an object read/written by the transaction, all of them should
			 * participate in the voting phase, and send their votes.
			 * 
			 * Not that in SDUR, read replicas should also receive votes, 
			 *  in order to remove the transaction from their {@link pendingTransactions} queue.  
			 */
			if (executionHistory.getTransactionType()!=TransactionType.INIT_TRANSACTION)
				keys.addAll(executionHistory.getReadSet().getKeys());
			
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
			return keys;
		}
	}
}
