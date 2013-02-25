package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.Learner;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.MULTICAST_MODE;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.termination.DistributedTermination;

/**
 * Abstract class for communication primitives during termination of a transaction.
 * This class is needed by {@link DistributedTermination} 
 * 
 * @author Masoud Saeida Ardekani
 *
 */
public abstract class TerminationCommunication {

	protected JessyGroupManager manager = JessyGroupManager.getInstance();
	
	VoteMulticast voteMulticast;
	
	public TerminationCommunication(Learner fractalLearner, UnicastLearner nettyLearner) {
		
			if (ConstantPool.JESSY_VOTING_PHASE_MULTICAST_MODE == MULTICAST_MODE.FRACTAL) {
				voteMulticast=new VoteMulticastWithFractal(fractalLearner,nettyLearner);
			} else {
				voteMulticast=new VoteMulticastWithNetty(nettyLearner);
			}
	}
	
	public void sendVote(VoteMessage voteMessage,
			boolean isCertifyAtCoordinator, int coordinatorSwid,
			String coordinatorHost) {

		voteMulticast.sendVote(voteMessage, isCertifyAtCoordinator, coordinatorSwid, coordinatorHost);
	}

	/**
	 * Cast the message to the replicas concerned by the transaction. I.e, replicas
	 * whom replicate a key in the set returned by:
	 * {@code Consistency#getConcerningKeys(fr.inria.jessy.transaction.ExecutionHistory)}
	 */
	public abstract void terminateTransaction(
			ExecutionHistory ex, Collection<String> gDest, String gSource, int swidSource);
	
	public void close(){
		voteMulticast.close();
	}
}
