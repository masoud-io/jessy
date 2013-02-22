package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.ExecutionHistory;


public class GenuineTerminationCommunication extends TerminationCommunication{

	/**
	 * Stream used for atomic multicast messages
	 */
	protected WanAMCastStream aMCastStream;
	
	public GenuineTerminationCommunication(Group group,  Learner learner) {
		super(learner);
		
		aMCastStream = FractalManager.getInstance()
				.getOrCreateWanAMCastStream(group.name(), group.name());
		aMCastStream.registerLearner("TerminateTransactionRequestMessage", learner);
		aMCastStream.start();
	}

	@Override
	public void terminateTransaction(ExecutionHistory ex,
			Collection<String> gDest, String gSource, int swidSource) {
		aMCastStream.atomicMulticast(new TerminateTransactionRequestMessage(ex,gDest,gSource,swidSource));
	}

}
