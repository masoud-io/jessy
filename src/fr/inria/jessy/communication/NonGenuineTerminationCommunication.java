package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.abcast.ABCastStream;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream;
import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;

public class NonGenuineTerminationCommunication extends
		TerminationCommunication {

	/**
	 * Stream used for atomic broadcast messages
	 */
	protected GPaxosStream gpaxosStream;

	public NonGenuineTerminationCommunication(Group group, Group all,
			Learner learner, Collection<String> allGroup) {
		super(group, all, learner);
//		gpaxosStream = FractalManager.getInstance().getOrCreateGPaxosStream(
//				"gpaxosStream", all.name(), acceptorGroupName, learnerGroupName, cstructClassName, 
//				useFastBallot, recovery, ballotTimeOut, checkpointSize)
//				"aBCastStream", "aBCastStream", "aBCastStream");
		gpaxosStream.registerLearner("WanABCastIntraGroupMessage",
				learner);
		gpaxosStream.start();

	}

	@Override
	public void sendTerminateTransactionRequestMessage(
			TerminateTransactionRequestMessage msg, Collection<String> dest) {
//		gpaxosStream.atomicBroadcast(msg);

	}

}
