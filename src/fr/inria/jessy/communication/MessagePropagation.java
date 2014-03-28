package fr.inria.jessy.communication;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.multicast.MulticastMessage;
import net.sourceforge.fractal.multicast.MulticastStream;

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

	public MessagePropagation(String msgType, Learner learner, JessyGroupManager m) {
		mCastStream = m.fractal.getOrCreateMulticastStream(
				m.getMyGroup().name(),
				m.getMyGroup().name());
		mCastStream.registerLearner(msgType, learner);
		mCastStream.start();
	}

	/**
	 * Multicast the message msg
	 * 
	 * TODO java.util.ConcurrentModificationException if it is not synchronized
	 * 
	 * @param msg
	 */
	public void propagate(MulticastMessage msg) {
		mCastStream.multicast(msg);
	}

}
