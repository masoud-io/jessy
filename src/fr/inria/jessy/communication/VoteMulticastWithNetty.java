package fr.inria.jessy.communication;

import java.util.Collection;
import java.util.Set;

import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.message.VoteMessage;

public class VoteMulticastWithNetty extends VoteMulticast{

	private UnicastClientManager cManager;
	private UnicastServerManager sManager;
	private DistributedJessy distributedJessy;
	
	public VoteMulticastWithNetty(DistributedJessy j, UnicastLearner learner) {

		sManager=new UnicastServerManager(j, learner, ConstantPool.JESSY_NETTY_VOTING_PHASE_PORT);
		distributedJessy=j;

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
			if (!distributedJessy.manager.isProxy()){
				initializecManager(distributedJessy,null,
						ConstantPool.JESSY_NETTY_VOTING_PHASE_PORT, distributedJessy.manager.getAllReplicaGroup().allNodes());
			}
		}

		multiCast(voteMessage, voteMessage.dest);
		
		if (!isCertifyAtCoordinator) {
			cManager.unicast(voteMessage, coordinatorSwid, coordinatorHost);
		}

	}
	
	public void sendVote(VoteMessage voteMessage,int  swid, String host) {
		if (cManager ==null){
			initializecManager(distributedJessy,null,
						ConstantPool.JESSY_NETTY_VOTING_PHASE_PORT, distributedJessy.manager.getAllReplicaGroup().allNodes());
		}
		
		cManager.unicast(voteMessage, swid, host);
	}
	
	private synchronized void initializecManager(DistributedJessy j, UnicastLearner learner, int port,
			Set<Integer> server_swid){
		if (cManager==null)
			cManager = new UnicastClientManager(distributedJessy,null,
					ConstantPool.JESSY_NETTY_VOTING_PHASE_PORT, distributedJessy.manager.getAllReplicaGroup().allNodes());
	}
	
	private void multiCast(Object obj, Collection<String> dest){
		for (String g:dest){
			for (Integer swid:distributedJessy.manager.getMembership().group(g).allNodes()){
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