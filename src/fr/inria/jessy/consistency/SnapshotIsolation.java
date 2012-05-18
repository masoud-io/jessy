package fr.inria.jessy.consistency;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.EntitySet;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;

public class SnapshotIsolation implements Consistency{

	private Map<AtomicInteger, EntitySet> committedWritesets;


	@Override
	public boolean certify(ExecutionHistory executionHistory) {

		if(executionHistory.getWriteSet().size()==0||
				executionHistory.getTransactionHandler().getTransactionSeqNumber()==Jessy.lastCommittedTransactionSeqNumber){
			//			any committed concurrent transactions
			return true;
		}

		EntitySet concurrentWS = getAllConcurrentWriteSets();

		Iterator<? extends JessyEntity> WSiterator = executionHistory.getWriteSet().getEntities().iterator();
		while(WSiterator.hasNext()){

			JessyEntity nextEntity = WSiterator.next();

			//			TODO check contains mathod
			if(concurrentWS.contains(nextEntity.getClass(), nextEntity.getKey())){
				//				ws intersection
				return false;
			}
		}
		//		no ws intersection
		return true;
	}

	@Override
	public boolean hasConflict(ExecutionHistory history1,
			ExecutionHistory history2) {

		new Exception("ERROR: hasConflict(ExecutionHistory history1, ExecutionHistory history2) " +
		"is called on SnapshotIsolation consistency criterion");

		return false;
	}

	/**
	 * 
	 * @param jessy the jessi instance
	 * @return writesets of transactions with sequence number equal or greater than the LastCommittedTransactionSeqNumber
	 */
	private EntitySet getAllConcurrentWriteSets(){

		boolean stop = false;

		EntitySet result= new EntitySet();
		int actualTransactionSeqNumber = Jessy.lastCommittedTransactionSeqNumber.get();
		while(!stop){
			EntitySet eh = committedWritesets.get(actualTransactionSeqNumber);

			if(eh==null){
				stop=true;
			}
			else{
				result.addEntity(eh);
				actualTransactionSeqNumber++;
			}
		}
		return result;
	}

	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {
		Jessy.lastCommittedTransactionSeqNumber.incrementAndGet();
		committedWritesets.put(Jessy.lastCommittedTransactionSeqNumber, executionHistory.getWriteSet());

	}

}
