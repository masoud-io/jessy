package fr.inria.jessy.communication;

import fr.inria.jessy.communication.message.VoteMessage;

public abstract class VoteMulticast {
	
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
	public abstract void sendVote(VoteMessage voteMessage,
			boolean isCertifyAtCoordinator, int coordinatorSwid,
			String coordinatorHost);
	
	public abstract void sendVote(VoteMessage voteMessage,int swid, String host) ;
	
	public abstract void close();
}
