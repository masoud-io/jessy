package fr.inria.jessy.communication;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.multicast.MulticastStream;
import fr.inria.jessy.communication.message.VectorMessage;

/**
 * In Genuine implementations of protocols such as PSI, and in order to ensure
 * progress, it is needed that each Jessy instance propagate its {@code Vector}
 * to other instance. This propagation is performed by this class.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class VectorPropagation {
	private MulticastStream mCastStream;

	public VectorPropagation(Learner learner) {
		mCastStream = FractalManager.getInstance().getOrCreateMulticastStream(
				JessyGroupManager.getInstance().getMyGroup().name(),
				JessyGroupManager.getInstance().getMyGroup().name());
		mCastStream.registerLearner("VectorMessage", learner);
		mCastStream.start();
	}

	/**
	 * Multicast the msg
	 * 
	 * TODO java.util.ConcurrentModificationException if it is not synchronized
	 * 
	 * @param msg
	 */
	public void propagate(VectorMessage msg) {
		mCastStream.multicast(msg);
	}

}
