package fr.inria.jessy.consistency;

import static fr.inria.jessy.transaction.ExecutionHistory.TransactionType.BLIND_WRITE;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import fr.inria.jessy.communication.GenuineTerminationCommunication;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.termination.Vote;
import fr.inria.jessy.transaction.termination.VotePiggyback;
import fr.inria.jessy.vector.GMUVector;

/**
 * This class implements Non-Monotonic Snapshot Isolation consistency criterion
 * along with using GMUVector introduced by [Peluso2012]
 * 
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class NonMonotonicSnapshotIsolationWithGMUVector extends Consistency {

	private static Logger logger = Logger
			.getLogger(NonMonotonicSnapshotIsolationWithGMUVector.class);

	private static ConcurrentHashMap<UUID, GMUVector<String>> receivedVectors;

	static {
		votePiggybackRequired = true;
		receivedVectors = new ConcurrentHashMap<UUID, GMUVector<String>>();
	}

	public NonMonotonicSnapshotIsolationWithGMUVector(DataStore dataStore) {
		super(dataStore);
	}

	public boolean certify(ExecutionHistory executionHistory) {
		TransactionType transactionType = executionHistory.getTransactionType();

		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ transactionType.toString());
		logger.debug("ReadSet Vector"
				+ executionHistory.getReadSet().getCompactVector().toString());
		logger.debug("CreateSet Vectors"
				+ executionHistory.getCreateSet().getCompactVector().toString());
		logger.debug("WriteSet Vectors"
				+ executionHistory.getWriteSet().getCompactVector().toString());

		/*
		 * if the transaction is a read-only transaction, it commits right away.
		 */
		if (transactionType == TransactionType.READONLY_TRANSACTION) {
			logger.debug(executionHistory.getTransactionHandler() + " >> "
					+ transactionType.toString() + " >> COMMITTED");
			return true;
		}

		/*
		 * if the transaction is an initalization transaction, it first
		 * increaments the vectors and then commits.
		 */
		if (transactionType == TransactionType.INIT_TRANSACTION) {

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

		JessyEntity lastComittedEntity;
		for (JessyEntity tmp : executionHistory.getWriteSet().getEntities()) {

			try {

				lastComittedEntity = store
						.get(new ReadRequest<JessyEntity>(
								(Class<JessyEntity>) tmp.getClass(),
								"secondaryKey", tmp.getKey(), null))
						.getEntity().iterator().next();

				/*
				 * instead of locking, we simply checks agains the latest
				 * committed values
				 */
				if (lastComittedEntity.getLocalVector().getSelfValue() > tmp
						.getLocalVector().getSelfValue()) {
					System.out.println("DAMN FALSE");
					return false;
				}

			} catch (NullPointerException e) {
				// nothing to do.
				// the key is simply not there.
			}

		}
		logger.debug(executionHistory.getTransactionHandler() + " >> "
				+ transactionType.toString() + " >> COMMITTED");

		return true;
	}

	@Override
	public boolean certificationCommute(ExecutionHistory history1,
			ExecutionHistory history2) {

		return false;
	}

	@Override
	public Vote createCertificationVote(ExecutionHistory executionHistory) {
		/*
		 * First, it needs to run the certification test on the received
		 * execution history. A blind write always succeeds.
		 */

		boolean isCommitted = executionHistory.getTransactionType() == BLIND_WRITE
				|| certify(executionHistory);

		GMUVector<String> prepVC = null;
		if (isCommitted
				&& executionHistory.getTransactionType() == TransactionType.UPDATE_TRANSACTION) {
			/*
			 * We have to update the vector here, and send it over to the
			 * others. Corresponds to line 20-22 of Algorithm 4
			 */
			prepVC = GMUVector.mostRecentVC.clone();
			Integer prepVCAti = GMUVector.lastPrepSC.incrementAndGet();
			prepVC.setValue(prepVC.getSelfKey(), prepVCAti);
		}

		/*
		 * Corresponds to line 23
		 */
		return new Vote(executionHistory.getTransactionHandler(), isCommitted,
				JessyGroupManager.getInstance().getMyGroup().name(),
				new VotePiggyback(prepVC));
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void voteReceived(Vote vote) {
		if (vote.getVotePiggyBack().getPiggyback() == null) {
			/*
			 * transaction has been terminated, or it is an init or read only
			 * transaction, thus a null piggyback is sent along with the vote.
			 * no need for extra work.
			 */
			return;
		}

		try {
			if (vote.getVotePiggyBack() != null) {

				GMUVector<String> vector = (GMUVector<String>) vote
						.getVotePiggyBack().getPiggyback();

				/*
				 * Corresponds to line 19
				 */
				if (receivedVectors.contains(vote.getTransactionHandler()
						.getId())) {
					receivedVectors.get(vote.getTransactionHandler().getId())
							.update(vector);
				} else {
					receivedVectors.put(vote.getTransactionHandler().getId(),
							vector);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public void prepareToCommit(ExecutionHistory executionHistory) {

		if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
			return;

		executionHistory.getWriteSet().addEntity(
				executionHistory.getCreateSet());
		
		/*
		 * Corresponds to line 22
		 */
		GMUVector<String> commitVC = receivedVectors.contains(executionHistory
				.getTransactionHandler().getId()) ? receivedVectors
				.get(executionHistory.getTransactionHandler().getId())
				: new GMUVector<String>();
		Integer xactVN = 0;
		for (Entry<String, Integer> entry : commitVC.getEntrySet()) {
			if (entry.getValue() > xactVN)
				xactVN = entry.getValue();
		}

		/*
		 * Corresponds to line 24
		 */
		Set<String> dest = new HashSet<String>(JessyGroupManager
				.getInstance()
				.getPartitioner()
				.resolveNames(
						getConcerningKeys(executionHistory,
								ConcernedKeysTarget.EXCHANGE_VOTES)));
		for (String index : dest) {
			commitVC.setValue(index, xactVN);
		}

		for (JessyEntity entity : executionHistory.getWriteSet().getEntities()) {
			entity.setLocalVector(commitVC);
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
			GMUVector<String> commitVC = receivedVectors.get(executionHistory
					.getTransactionHandler().getId());
			if (GMUVector.lastPrepSC.get() < commitVC.getSelfValue()) {
				GMUVector.lastPrepSC.set(commitVC.getSelfValue());
			}
		}

		/*
		 * Garbage collect the received vectors. We don't need them anymore.
		 */
		if (receivedVectors.contains(executionHistory.getTransactionHandler()
				.getId()))
			receivedVectors.remove(executionHistory.getTransactionHandler()
					.getId());
	}

	@Override
	public Set<String> getConcerningKeys(ExecutionHistory executionHistory,
			ConcernedKeysTarget target) {
		Set<String> keys = new HashSet<String>();
		keys.addAll(executionHistory.getWriteSet().getKeys());
		keys.addAll(executionHistory.getCreateSet().getKeys());
		return keys;
	}

	@Override
	public TerminationCommunication getOrCreateTerminationCommunication(
			Group group, Learner learner) {
		if (terminationCommunication == null)
			terminationCommunication = new GenuineTerminationCommunication(
					group, learner);
		return terminationCommunication;
	}
}
