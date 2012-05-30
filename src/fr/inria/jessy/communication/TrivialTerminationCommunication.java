package fr.inria.jessy.communication;

import java.io.Serializable;
import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.multicast.MulticastMessage;
import net.sourceforge.fractal.multicast.MulticastStream;
import fr.inria.jessy.transaction.termination.message.TerminateTransactionRequestMessage;

/**
 * This class implements a trivial termination protocol. I.e., multicast the
 * terminate transaction message. Thus, this class is not safe!
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class TrivialTerminationCommunication extends TerminationCommunication implements Learner{

	/**
	 * Stream used for multicast messages
	 */
	protected MulticastStream mCastTransaction;
	private Learner realLearner;

	
	public TrivialTerminationCommunication(Group group, Group all, Learner learner) {
		super(group, all, learner);
		mCastTransaction = FractalManager.getInstance().getOrCreateMulticastStream(
				group.name(), group.name());
		mCastTransaction.registerLearner("MulticastMessage", this);
		mCastTransaction.start();
		realLearner = learner;

	}

	
	@Override
	public void sendTerminateTransactionRequestMessage(
			TerminateTransactionRequestMessage msg, Collection<String> dest) {
		mCastTransaction.multicast(msg, dest);
		
	}


	@Override
	public void learn(Stream arg0, Serializable s) {
		realLearner.learn(arg0, ((MulticastMessage)s).serializable);
		
	}

}
