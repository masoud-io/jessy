package fr.inria.jessy.consistency;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.EntitySet;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;

public class ParallelSnapshotIsalation extends Consistency{
	
	private static Logger logger = Logger
	.getLogger(SnapshotIsolation.class);

	public ParallelSnapshotIsalation(DataStore store) {
		super(store);
		// TODO Auto-generated constructor stub
	}

//	 TODO check
	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {
		Set<String> history2Keys = history2.getWriteSet().getKeys();

		for (String key : history1.getWriteSet().getKeys()) {
			if (history2Keys.contains(key)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean certify(ExecutionHistory executionHistory) {
		
//		work in progress...
		if(true){
			return false;
		}
		
		logger.debug(executionHistory.getTransactionHandler() + " >> " + executionHistory.getTransactionType().toString());
		logger.debug("ReadSet Vector"
				+ executionHistory.getReadSet().getCompactVector().toString());
		logger.debug("WriteSet Vectors"
				+ executionHistory.getWriteSet().getCompactVector().toString());

		if (executionHistory.getTransactionType() == TransactionType.INIT_TRANSACTION) {

			// no conflicts can happen
			logger.debug(executionHistory.getTransactionHandler() + " >> "
					+ "COMMIT");
			return true;
		}

		if ((executionHistory.getWriteSet().size() == 0 && executionHistory.getCreateSet().size() == 0)) {

				logger.debug(executionHistory.getTransactionHandler()
						+ " empty writeSet and createSet >> " + "COMMIT");

			return true;
		}

//		TODO 
		
//		EntitySet concurrentWS = getAllConcurrentWriteSets(executionHistory
//				.getTransactionHandler().getTransactionSeqNumber());
//
//		Iterator<? extends JessyEntity> WSiterator = executionHistory
//				.getWriteSet().getEntities().iterator();
//		while (WSiterator.hasNext()) {
//
//			JessyEntity nextEntity = WSiterator.next();
//
//			// TODO check contains method
//			if (concurrentWS.contains(nextEntity.getKey())) {
//				// ws intersection
//
//				logger.debug(executionHistory.getTransactionHandler()
//						+ " ws intersection >> " + "ABORT");
//
//				return false;
//			}
//		}
//		// no ws intersection
//		logger.debug(executionHistory.getTransactionHandler()
//				+ " NO ws intersection >> " + "COMMIT");
//		return true;
		
		return true;
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {
//		 TODO Auto-generated method stub
		
	}

	@Override
	public TerminationCommunication getOrCreateTerminationCommunication(
			Group group, Group all, Learner learner,
			Collection<String> allGroupNames) {
		// TODO Auto-generated method stub
		return null;
	}

}
