package fr.inria.jessy.communication;

import java.util.Collection;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.message.VoteMessage;

public class VoteMulticastWithNetty extends VoteMulticast{

	private UnicastClientManager cManager;
	private UnicastServerManager sManager;
	
	public VoteMulticastWithNetty(UnicastLearner learner) {

		sManager=new UnicastServerManager(learner, ConstantPool.JESSY_NETTY_VOTING_PHASE_PORT);

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
		if (cManager ==null){
			if (!manager.isProxy()){
				cManager = new UnicastClientManager(null,
						ConstantPool.JESSY_NETTY_VOTING_PHASE_PORT, manager.getAllReplicaGroup().allNodes());
			}
		}

		multiCast(voteMessage, voteMessage.dest);
		
		if (!isCertifyAtCoordinator) {
			cManager.unicast(voteMessage, coordinatorSwid, coordinatorHost);
		}

	}
	
	private void multiCast(Object obj, Collection<String> dest){
		for (String g:dest){
			for (Integer swid:manager.getMembership().group(g).allNodes()){
				cManager.unicast(obj, swid);
			}
		}
	}

	@Override
	public void close() {
		if (cManager!=null)
			cManager.close();
		
		sManager.close();
	}

}
