package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.wanabcast.WanABCastStream;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;

public class NonGenuineTerminationCommunication extends
		TerminationCommunication {

	/**
	 * Stream used for atomic broadcast messages
	 */
	protected WanABCastStream aBCastStream;

	public NonGenuineTerminationCommunication(Group group, Group all,
			Learner learner, Collection<String> allGroupNames) {
		super(group, all, learner);
		aBCastStream = FractalManager.getInstance().getOrCreateWanABCastStream(
				"TerminateTransactionRequestMessage", allGroupNames, group.name(),
				ConstantPool.MAX_INTERGROUP_MESSAGE_DELAY,
				ConstantPool.CONSENSUS_LATENCY);
		aBCastStream.registerLearner("TerminateTransactionRequestMessage",
				learner);
		aBCastStream.start();

	}

	@Override
	public void sendTerminateTransactionRequestMessage(
			TerminateTransactionRequestMessage msg, Collection<String> dest) {
		aBCastStream.atomicBroadcast(msg);

	}

}
