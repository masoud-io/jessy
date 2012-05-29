package fr.inria.jessy.communication;

import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.multicast.MulticastStream;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;

/**
 * This class implements a trivial termination protocol. I.e., multicast the
 * terminate transaction message. Thus, this class is not safe!
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class TrivialTerminationCommunication extends TerminationCommunication{

	/**
	 * Stream used for multicast messages
	 */
	protected MulticastStream mCastTransaction;

	
	public TrivialTerminationCommunication(Group group, Group all, Learner learner) {
		super(group, all, learner);
		mCastStream = FractalManager.getInstance().getOrCreateMulticastStream(
				group.name(), group.name());
		mCastTransaction.registerLearner("MulticastMessage", learner);
		mCastTransaction.start();

	}

	@Override
	public void sendTerminateTransactionRequestMessage(
			TerminateTransactionRequestMessage msg, Collection<String> dest) {
		mCastStream.multicast(msg, dest);
		
	}

}
