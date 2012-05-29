package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;


public class GenuineTerminationCommincation extends TerminationCommunication{

	/**
	 * Stream used for atomic multicast messages
	 */
	protected WanAMCastStream aMCastStream;
	
	public GenuineTerminationCommincation(String groupName, Learner learner) {
		super(groupName, learner);
		
		aMCastStream = FractalManager.getInstance()
				.getOrCreateWanAMCastStream(groupName, groupName);
		aMCastStream.registerLearner("TerminateTransactionRequestMessage", learner);
		aMCastStream.start();
	}

	@Override
	public void sendTerminateTransactionRequestMessage(
			TerminateTransactionRequestMessage msg, Collection<String> dest) {
		aMCastStream.atomicMulticast(msg);
	}

}
