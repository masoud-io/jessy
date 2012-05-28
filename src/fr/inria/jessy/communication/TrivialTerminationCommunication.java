package fr.inria.jessy.communication;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
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

	
	public TrivialTerminationCommunication(String groupName, Learner learner) {
		super(groupName, learner);
		mCastStream = FractalManager.getInstance().getOrCreateMulticastStream(
				groupName, groupName);
		mCastTransaction.registerLearner("TerminateTransactionRequestMessage", learner);
		mCastTransaction.start();

	}

	@Override
	public void sendTerminateTransactionRequestMessage(
			TerminateTransactionRequestMessage msg) {
//		mCastStream.multicast(msg);
		
	}

}
