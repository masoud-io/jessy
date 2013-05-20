package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.utils.CollectionUtils;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.protocol.US_DV_GC;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.TransactionTouchedKeys;

public abstract class US extends Consistency {
	
	protected static Logger logger = Logger
			.getLogger(US_DV_GC.class);
	
	public US(JessyGroupManager m, DataStore store) {
		super(m, store);
	}

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
		
	}
	
	@Override
	public boolean certificationCommute(TransactionTouchedKeys tk1,
			TransactionTouchedKeys tk2) {
		boolean result=true;;
		
		if (tk1.readKeys!=null && tk2.writeKeys!=null){
			result = !CollectionUtils.isIntersectingWith(tk2.writeKeys, tk1.readKeys);
		}
		if (tk1.writeKeys!=null && tk2.readKeys!=null){
			result = result && !CollectionUtils.isIntersectingWith(tk1.writeKeys, tk2.readKeys);
		}
		
		return result;
	}

	
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
				 * should atomic multicast to every process replicating an
				 * object read or written by the transaction.
				 * 
				 * Note: Atomic multicasting to only write-set is not enough.
				 */
				keys.addAll(executionHistory.getReadSet().getKeys());
				keys.addAll(executionHistory.getWriteSet().getKeys());
				keys.addAll(executionHistory.getCreateSet().getKeys());
				return keys;
			}
		} else if (target == ConcernedKeysTarget.SEND_VOTES) {
			/*
			 * Since the transaction is sent to all jessy instances replicating
			 * an object read/written by the transaction, all of them should
			 * participate in the voting phase, and send their votes.
			 */
			if (executionHistory.getTransactionType()!=TransactionType.INIT_TRANSACTION)
				keys.addAll(executionHistory.getReadSet().getKeys());
			
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
			return keys;
		} else {
			/*
			 * For exchanging votes, it is only needed to send the result of the
			 * transaction to its write set.
			 */
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
			return keys;
		}
	}
	
}
