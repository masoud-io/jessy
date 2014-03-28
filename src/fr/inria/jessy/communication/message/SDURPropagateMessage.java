package fr.inria.jessy.communication.message;

import java.util.Set;

import net.sourceforge.fractal.multicast.MulticastMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.protocol.SDURPiggyback;

/**
 * Used for propagating the committed sequence number to other replicas
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class SDURPropagateMessage extends MulticastMessage {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	@Deprecated
	public SDURPropagateMessage() {
	}

	public SDURPropagateMessage(
			SDURPiggyback piggyback, Set<String> dest,
			String gsource, int source) {
		super(piggyback, dest, gsource, source);
	}

	public SDURPiggyback getSDURPiggyback() {
		return (SDURPiggyback) serializable;
	}

}
