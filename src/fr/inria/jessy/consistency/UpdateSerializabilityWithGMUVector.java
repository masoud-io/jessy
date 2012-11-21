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
import fr.inria.jessy.consistency.Consistency.ConcernedKeysTarget;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionType;
import fr.inria.jessy.transaction.termination.Vote;
import fr.inria.jessy.transaction.termination.VotePiggyback;
import fr.inria.jessy.vector.GMUVector;

/**
 * This class implements Update Serializability consistency criterion along with
 * using GMUVector introduced by [Peluso2012]
 * 
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class UpdateSerializabilityWithGMUVector extends Consistency {

	private static Logger logger = Logger
			.getLogger(UpdateSerializabilityWithGMUVector.class);

	private static ConcurrentHashMap<UUID, GMUVector<String>> receivedVectors;

	static {
		votePiggybackRequired = true;
		receivedVectors = new ConcurrentHashMap<UUID, GMUVector<String>>();
	}

	public UpdateSerializabilityWithGMUVector(DataStore dataStore) {
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
				 * instead of locking, we simply checks against the latest
				 * committed values
				 */
				if (lastComittedEntity.getLocalVector().getSelfValue() > tmp
						.getLocalVector().getSelfValue())
					return false;

			} catch (NullPointerException e) {
				// nothing to do.
				// the key is simply not there.
			}

		}

		for (JessyEntity tmp : executionHistory.getCreateSet().getEntities()) {

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
						.getLocalVector().getSelfValue())
					return false;

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
		if (isCommitted) {
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
			 * transaction has been terminated, thus a null piggyback is sent
			 * along with the vote. no need for extra work.
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

		/*
		 * Corresponds to line 22
		 */
		GMUVector<String> commitVC = receivedVectors.get(executionHistory
				.getTransactionHandler().getId());
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
								ConcernedKeysTarget.RECEIVE_VOTES)));
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
		if (target == ConcernedKeysTarget.TERMINATION_CAST) {
			if (executionHistory.getTransactionType() == TransactionType.READONLY_TRANSACTION)
				/*
				 * If the transaction is readonly, it is not needed to be atomic
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
			keys.addAll(executionHistory.getReadSet().getKeys());
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
			return keys;
		} else {
			/*
			 * For exchanging votes, it is only needed to send the result of the
			 * transaction to its writeset.
			 */
			keys.addAll(executionHistory.getWriteSet().getKeys());
			keys.addAll(executionHistory.getCreateSet().getKeys());
			return keys;
		}
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
