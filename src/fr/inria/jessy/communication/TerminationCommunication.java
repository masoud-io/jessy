package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.Learner;
import fr.inria.jessy.DistributedJessy;
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

	protected JessyGroupManager manager;
	VoteMulticast voteMulticast;
	protected DistributedJessy j;
	
	public TerminationCommunication(DistributedJessy jessy, Learner fractalLearner) {
			j = jessy;
			manager = j.manager;
			voteMulticast=new VoteMulticastWithFractal(fractalLearner,jessy);
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
