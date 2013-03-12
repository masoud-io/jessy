package fr.inria.jessy.communication;

import java.util.Collection;

import javax.management.RuntimeErrorException;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.termination.DistributedTermination;

/**
 * This class provides a traditional genuine termination communication
 * primitives that are needed for terminating a transaction in {@link DistributedTermination}
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class GenuineTerminationCommunication extends TerminationCommunication {

	/**
	 * Stream used for atomic multicast messages
	 */
	protected WanAMCastStream aMCastStream;

	public GenuineTerminationCommunication(Group group, Learner fractalLearner,
			UnicastLearner nettyLearner) {
		super(fractalLearner, nettyLearner);

		aMCastStream = FractalManager.getInstance().getOrCreateWanAMCastStream(
				group.name(), group.name());
		aMCastStream.registerLearner("TerminateTransactionRequestMessage",
				fractalLearner);
		aMCastStream.start();
	}

	@Override
	public void terminateTransaction(ExecutionHistory ex,
			Collection<String> gDest, String gSource, int swidSource) {
		aMCastStream.atomicMulticast(new TerminateTransactionRequestMessage(ex,
				gDest, gSource, swidSource));
	}

}
