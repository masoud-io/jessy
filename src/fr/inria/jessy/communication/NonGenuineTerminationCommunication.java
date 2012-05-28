package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.wanabcast.WanABCastStream;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;

public class NonGenuineTerminationCommunication extends
		TerminationCommunication {

	/**
	 * Stream used for atomic broadcast messages
	 */
	protected WanABCastStream aBCastStream;

	public NonGenuineTerminationCommunication(String groupName,
			Learner learner, Collection<String> allGroupNames) {
		super(groupName, learner);
		aBCastStream = FractalManager.getInstance().getOrCreateWanABCastStream(
				"TerminateTransactionRequestMessage", allGroupNames, groupName,
				ConstantPool.MAX_INTERGROUP_MESSAGE_DELAY,
				ConstantPool.CONSENSUS_LATENCY);
		aBCastStream.registerLearner("TerminateTransactionRequestMessage",
				learner);
		aBCastStream.start();

	}

	@Override
	public void sendTerminateTransactionRequestMessage(
			TerminateTransactionRequestMessage msg) {
		aBCastStream.atomicBroadcast(msg);

	}

}
