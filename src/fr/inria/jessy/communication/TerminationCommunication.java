package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.Learner;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.termination.AtomicCommit;

/**
 * Abstract class for communication primitives during termination of a transaction.
 * This class is needed by {@link AtomicCommit} 
 * 
 * @author Masoud Saeida Ardekani
 *
 */
public abstract class TerminationCommunication {

	protected JessyGroupManager manager;
	
	protected DistributedJessy j;
	
	public TerminationCommunication(DistributedJessy jessy, Learner fractalLearner, UnicastLearner nettyLearner) {
			j = jessy;
			manager = j.manager;

	}
	
	/**
	 * Cast the message to the replicas concerned by the transaction. I.e, replicas
	 * whom replicate a key in the set returned by:
	 * {@code Consistency#getConcerningKeys(fr.inria.jessy.transaction.ExecutionHistory)}
	 */
	public abstract void terminateTransaction(
			ExecutionHistory ex, Collection<String> gDest, String gSource, int swidSource);
	
}
