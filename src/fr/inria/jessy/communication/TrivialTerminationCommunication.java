package fr.inria.jessy.communication;

import java.io.Serializable;
import java.util.Collection;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.multicast.MulticastMessage;
import net.sourceforge.fractal.multicast.MulticastStream;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.ExecutionHistory;

/**
 * This class implements a trivial termination protocol. I.e., multicast the
 * terminate transaction message. Thus, this class is not safe!
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class TrivialTerminationCommunication extends TerminationCommunication implements Learner{

	public TrivialTerminationCommunication(
			Learner fractalLearner, UnicastLearner nettyLearner,
			DistributedJessy j) {
		super(j,fractalLearner,nettyLearner);
		mCastTransaction = j.manager.fractal.getOrCreateMulticastStream(
				"", manager.getMyGroup().name());
		mCastTransaction.registerLearner("MulticastMessage", this);
		mCastTransaction.start();
		realLearner = fractalLearner;
	}


	/**
	 * Stream used for multicast messages
	 */
	protected MulticastStream mCastTransaction;
	private Learner realLearner;

	@Override
	public void terminateTransaction(
			ExecutionHistory ex, Collection<String> gDest, String gSource, int swidSource) {
		mCastTransaction.multicast(new TerminateTransactionRequestMessage(ex,gDest,gSource,swidSource), gDest);
		
	}


	@Override
	public void learn(Stream arg0, Serializable s) {
		realLearner.learn(arg0, ((MulticastMessage)s).serializable);
		
	}

}
