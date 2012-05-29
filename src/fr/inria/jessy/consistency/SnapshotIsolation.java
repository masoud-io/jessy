package fr.inria.jessy.consistency;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.communication.NonGenuineTerminationCommunication;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.EntitySet;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;

//TODO COMMENT ME
public class SnapshotIsolation extends Consistency {

	private static Logger logger = Logger
			.getLogger(SnapshotIsolation.class);

	private Map<Integer, EntitySet> committedWritesets;

	public SnapshotIsolation(DataStore store) {
		super(store);
		committedWritesets = new HashMap<Integer, EntitySet>();
	}

	@Override
	public boolean certify(ExecutionHistory executionHistory) {

		// logger.debug(executionHistory.getTransactionHandler() +
		// " processing >> "
		// + executionHistory.getTransactionType().toString());
		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ executionHistory.getTransactionType().toString());
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

		if ((executionHistory.getWriteSet().size() == 0 && executionHistory
				.getCreateSet().size() == 0)
				|| executionHistory.getTransactionHandler()
						.getTransactionSeqNumber() == Jessy.lastCommittedTransactionSeqNumber
						.get()) {

			// any committed concurrent transactions
			if (executionHistory.getTransactionHandler()
					.getTransactionSeqNumber() == Jessy.lastCommittedTransactionSeqNumber
					.get()) {
				logger.debug(executionHistory.getTransactionHandler()
						+ " any committed concurrent transactions >> "
						+ "COMMIT");
			} else {
				logger.debug(executionHistory.getTransactionHandler()
						+ " empty writeSet and createSet >> " + "COMMIT");
			}

			return true;
		}

		// executionHistory.getWriteSet().addEntity(
		// executionHistory.getCreateSet());

		EntitySet concurrentWS = getAllConcurrentWriteSets(executionHistory
				.getTransactionHandler().getTransactionSeqNumber());

		Iterator<? extends JessyEntity> WSiterator = executionHistory
				.getWriteSet().getEntities().iterator();
		while (WSiterator.hasNext()) {

			JessyEntity nextEntity = WSiterator.next();

			// TODO check contains method
			if (concurrentWS.contains(nextEntity.getKey())) {
				// ws intersection

				logger.debug(executionHistory.getTransactionHandler()
						+ " ws intersection >> " + "ABORT");

				return false;
			}
		}
		// no ws intersection
		logger.debug(executionHistory.getTransactionHandler()
				+ " NO ws intersection >> " + "COMMIT");
		return true;
	}

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

	/**
	 * 
	 * @param transactionSequenceNumber
	 *            the sequence number of the transaction
	 * @return writesets of transactions with sequence number equal or greater
	 *         than the LastCommittedTransactionSeqNumber
	 */
	private EntitySet getAllConcurrentWriteSets(int transactionSequenceNumber) {

		boolean stop = false;

		EntitySet result = new EntitySet();
		int actualTransactionSeqNumber = transactionSequenceNumber + 1;

		// // handle special case of concurrent transactions on version 0
		// if(actualTransactionSeqNumber==0){
		// actualTransactionSeqNumber=1;
		// }

		while (!stop) {
			EntitySet eh = committedWritesets.get(actualTransactionSeqNumber);

			if (eh == null) {
				stop = true;
			} else {
				result.addEntity(eh);
				actualTransactionSeqNumber++;
			}
		}
		return result;
	}

	/**
	 * update (1) the {@link lastCommittedTransactionSeqNumber}: incremented by
	 * 1 (2) the {@link committedWritesets} with the new
	 * {@link lastCommittedTransactionSeqNumber} and {@link committedWritesets}
	 * (3) the scalar vector of all updated or created entities
	 */
	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {

		Jessy.lastCommittedTransactionSeqNumber.incrementAndGet();
		committedWritesets.put(Jessy.lastCommittedTransactionSeqNumber.get(),
				executionHistory.getWriteSet());

		// executionHistory.getWriteSet().addEntity(
		// executionHistory.getCreateSet());

		for (JessyEntity je : executionHistory.getWriteSet().getEntities()) {
			je.getLocalVector().update(null, null);
		}

		// for(JessyEntity je:executionHistory.getCreateSet().getEntities()){
		// je.getLocalVector().update(null, null);
		// }

		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ "COMMITED, lastCommittedTransactionSeqNumber:"
				+ Jessy.lastCommittedTransactionSeqNumber.get());
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory) {
		Set<String> keys = new HashSet<String>();
		keys.addAll(executionHistory.getWriteSet().getKeys());
		keys.addAll(executionHistory.getCreateSet().getKeys());
		return keys;
	}

	@Override
	public TerminationCommunication getOrCreateTerminationCommunication(
			Group group, Group all, Learner learner, Collection<String> allGroupNames) {
		if (terminationCommunication == null)
			terminationCommunication = new NonGenuineTerminationCommunication(
					group, all, learner, allGroupNames);
		return terminationCommunication;
	}

}
