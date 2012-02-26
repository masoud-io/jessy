package fr.inria.jessy.consistency;

import java.util.Iterator;
import java.util.List;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.vector.Vector;

public class NonMonotonicSnapshotIsolation implements Consistency {

	// TODO check if the transaction has read before write or not!!!
	@Override
	public <T extends Jessy> boolean certify(T jessy, ExecutionHistory executionHistory) {
		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
			return true;

		List<? extends JessyEntity> writeSet=executionHistory.getWriteSet(); 
//		
//		Vector<String> updatedVector=writeSet.get(0).getLocalVector().clone();				
//		updatedVector.update(executionHistory.getReadSetVectors(), executionHistory.getWriteSetVectors());
		
		Iterator<? extends JessyEntity> itr=writeSet.iterator();
		while(itr.hasNext()){
			JessyEntity tmp=itr.next();
			
			
		}
		
		// TODO Auto-generated method stub
		return false;
	}
}
