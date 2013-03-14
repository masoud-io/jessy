package fr.inria.jessy.communication;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.multicast.MulticastStream;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.message.ParallelSnapshotIsolationPropagateMessage;

/**
 * In Genuine implementations of protocols such as PSI, and in order to ensure
 * progress, it is needed that each Jessy instance propagate its {@code Vector}
 * to other instance. This propagation is performed by this class.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class MessagePropagation {
	private MulticastStream mCastStream;

	public MessagePropagation(Learner learner, JessyGroupManager m) {
		mCastStream = m.fractal.getOrCreateMulticastStream(
				m.getMyGroup().name(),
				m.getMyGroup().name());
		mCastStream.registerLearner("ParallelSnapshotIsolationPropagateMessage", learner);
		mCastStream.start();
	}

	/**
	 * Multicast the msg
	 * 
	 * TODO java.util.ConcurrentModificationException if it is not synchronized
	 * 
	 * @param msg
	 */
	public void propagate(ParallelSnapshotIsolationPropagateMessage msg) {
		mCastStream.multicast(msg);
	}

}
