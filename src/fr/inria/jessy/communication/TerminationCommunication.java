package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.multicast.MulticastStream;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.termination.message.VoteMessage;

public abstract class TerminationCommunication {

	/**
	 * Stream used for multicast messages
	 */
	protected MulticastStream mCastStream;
	protected Group myGroup;
	protected Group allGroup;

	public TerminationCommunication(Group group, Group all, Learner learner) {
		myGroup = group;
		allGroup = all;
		mCastStream = FractalManager.getInstance().getOrCreateMulticastStream(
				myGroup.name(), myGroup.name());
		mCastStream.registerLearner("VoteMessage", learner);
		mCastStream.start();
	}

	/**
	 * Multicast the voteMessage to the WriteSet of the transaction.
	 * <p>
	 * If the transaction coordinator is not among the receivers
	 * (isCertifyAtCoordinator is false), the voteMessage is also unicast to the
	 * coordinator. This optimization saves one message delay.
	 * 
	 * @param voteMessage
	 *            the VoteMessage to be multicast
	 */
	public void sendVote(VoteMessage voteMessage,
			boolean isCertifyAtCoordinator, int coordinatorId) {
		mCastStream.multicast(voteMessage);
		if (!isCertifyAtCoordinator)
			mCastStream.unicast(voteMessage, coordinatorId, allGroup);
	}

	/**
	 * Cast the msg to the replicas concerned by the transaction. I.e, replicas
	 * whom replicate a key in the set returned by:
	 * {@code Consistency#getConcerningKeys(fr.inria.jessy.transaction.ExecutionHistory)}
	 */
	public abstract void sendTerminateTransactionRequestMessage(
			TerminateTransactionRequestMessage msg, Collection<String> dest);
}
