package fr.inria.jessy.communication;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.multicast.MulticastStream;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.message.VoteMessage;

/**
 * Multicasting the vote messages using Fractal.
 * 
 * Note that the unicast of the vote to the coordinator is done with netty.
 * 
 * @author Masoud Saeida Ardekani
 *
 */
public class VoteMulticastWithFractal extends VoteMulticast{
	
	/**
	 * Stream used for multicast messages
	 */
	protected MulticastStream mCastStream;
	
	private DistributedJessy j;

	public VoteMulticastWithFractal(
			Learner fractalLearner,
			DistributedJessy jessy){
		j =jessy;
		mCastStream = j.manager.fractal.getOrCreateMulticastStream(
				ConstantPool.JESSY_VOTE_STREAM, j.manager.getMyGroup().name());
		mCastStream.registerLearner("VoteMessage", fractalLearner);
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
			boolean isCertifyAtCoordinator, int coordinatorSwid,
			String coordinatorHost) {

		mCastStream.multicast(voteMessage);
		if (!isCertifyAtCoordinator) {
			mCastStream.unicast(voteMessage, coordinatorSwid,
					j.manager.getEverybodyGroup());
		}

	}

	@Override
	public void close() {
	}

}
