package fr.inria.jessy.consistency;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.utils.CollectionUtils;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionTouchedKeys;

/**
 * Implements P-Store [Schiper2010]
 * 
 * CONS: SER
 * Vector: Null Vector
 * Atomic Commitment: GroupCommunication
 * 
 * @author Masoud Saeida Ardekani
 *
 */
public abstract class SER extends Consistency {

	protected static Logger logger = Logger.getLogger(SER.class);

	public SER(JessyGroupManager m, DataStore dateStore) {
		super(m, dateStore);
	}

	/**
	 * TODO Consider all cases
	 */
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
//		boolean result=true;
//		
//		if (tk1.readKeys!=null && tk2.writeKeys!=null){
//			result = !CollectionUtils.isIntersectingWith(tk2.writeKeys, tk1.readKeys);
//		}
//		if (tk1.writeKeys!=null && tk2.readKeys!=null){
//			result = result && !CollectionUtils.isIntersectingWith(tk1.writeKeys, tk2.readKeys);
//		}
//		
//		return result;
		return false;
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory,
			ConcernedKeysTarget target) {
		Set<String> keys = new HashSet<String>();
		if (target == ConcernedKeysTarget.TERMINATION_CAST ) {
			keys.addAll(executionHistory.getReadSet().getKeys());
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
		}else if(target == ConcernedKeysTarget.SEND_VOTES){
			
			if (executionHistory.getReadSet()!=null)
				keys.addAll(executionHistory.getReadSet().getKeys());
			
			if (executionHistory.getWriteSet()!=null)
				keys.addAll(executionHistory.getWriteSet().getKeys());
			
			if (executionHistory.getCreateSet()!=null)
				keys.addAll(executionHistory.getCreateSet().getKeys());
		}
		else {
			if (executionHistory.getWriteSet()!=null)
				keys.addAll(executionHistory.getWriteSet().getKeys());
			
			if (executionHistory.getCreateSet()!=null)
				keys.addAll(executionHistory.getCreateSet().getKeys());
		}
		return keys;
	}

}
