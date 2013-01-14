package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.multicast.MulticastStream;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.UNICAST_MODE;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;

public abstract class TerminationCommunication {

	protected JessyGroupManager manager = JessyGroupManager.getInstance();

	/**
	 * Stream used for multicast messages
	 */
	private MulticastStream mCastStream;

	private UnicastClientManager cManager;

	public TerminationCommunication(Learner learner) {
		mCastStream = FractalManager.getInstance().getOrCreateMulticastStream(
				ConstantPool.JESSY_VOTE_STREAM, manager.getMyGroup().name());
		mCastStream.registerLearner("VoteMessage", learner);
		mCastStream.start();

		cManager = new UnicastClientManager(null,
				ConstantPool.JESSY_NETTY_VOTING_PHASE_PORT, null);
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
			boolean isCertifyAtCoordinator, int coordinatorSwid,
			String coordinatorHost) {


		mCastStream.multicast(voteMessage);
		if (!isCertifyAtCoordinator) {
			if (ConstantPool.JESSY_VOTING_PHASE_UNICST_MODE == UNICAST_MODE.FRACTAL) {
				mCastStream.unicast(voteMessage, coordinatorSwid,
						manager.getEverybodyGroup());
			} else {

				cManager.unicast(voteMessage, coordinatorSwid, coordinatorHost);
			}
		}

	}

	/**
	 * Cast the message to the replicas concerned by the transaction. I.e, replicas
	 * whom replicate a key in the set returned by:
	 * {@code Consistency#getConcerningKeys(fr.inria.jessy.transaction.ExecutionHistory)}
	 */
	public abstract void sendTerminateTransactionRequestMessage(
			TerminateTransactionRequestMessage msg, Collection<String> dest);
}
