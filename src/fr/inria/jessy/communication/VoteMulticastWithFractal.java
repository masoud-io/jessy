package fr.inria.jessy.communication;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.multicast.MulticastStream;
import fr.inria.jessy.ConstantPool;
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
	
	private UnicastClientManager cManager;
	
	private UnicastServerManager sManager;

	public VoteMulticastWithFractal(Learner fractalLearner, UnicastLearner nettyLearner ){
		mCastStream = FractalManager.getInstance().getOrCreateMulticastStream(
				ConstantPool.JESSY_VOTE_STREAM, manager.getMyGroup().name());
		mCastStream.registerLearner("VoteMessage", fractalLearner);
		mCastStream.start();
		
//		if (manager.isProxy()){
//			sManager=new UnicastServerManager(nettyLearner, ConstantPool.JESSY_NETTY_VOTING_PHASE_PORT);
//		}
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
					manager.getEverybodyGroup());
//			if (cManager==null){
//				cManager = new UnicastClientManager(null,
//						ConstantPool.JESSY_NETTY_VOTING_PHASE_PORT, null);
//			}
//			cManager.unicast(voteMessage, coordinatorSwid, coordinatorHost);
		}

	}

	@Override
	public void close() {
//		if (cManager!=null)
//			cManager.close();
		
//		sManager.close();
	}

}
