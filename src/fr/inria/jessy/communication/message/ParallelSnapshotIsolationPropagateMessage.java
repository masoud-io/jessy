package fr.inria.jessy.communication.message;

import java.util.Set;

import net.sourceforge.fractal.multicast.MulticastMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.consistency.ParallelSnapshotIsolationPiggyback;

/**
 * Used for propagating the committed sequence number of the WCoordinator to all
 * jessy instances in the system except those that are concerned by the
 * committed transaction. Because those that are concerned by the committed
 * transaction have already received the sequence number through the
 * {@code ParallelSnapshotIsolationPiggyback} used in {@code Vote}.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class ParallelSnapshotIsolationPropagateMessage extends MulticastMessage {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	@Deprecated
	public ParallelSnapshotIsolationPropagateMessage() {
	}

	public ParallelSnapshotIsolationPropagateMessage(
			ParallelSnapshotIsolationPiggyback piggyback, Set<String> dest,
			String gsource, int source) {
		super(piggyback, dest, gsource, source);
	}

	public ParallelSnapshotIsolationPiggyback getParallelSnapshotIsolationPiggyback() {
		return (ParallelSnapshotIsolationPiggyback) serializable;
	}

}
