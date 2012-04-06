package fr.inria.jessy;

import java.util.Set;

import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.rmcast.WanMessage;
import fr.inria.jessy.store.ReadRequest;

public class RemoteReadRequestMessage extends WanMessage {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	public RemoteReadRequestMessage() {
	}

	public RemoteReadRequestMessage(ReadRequest r, Set<String> dest) {
		super(r, dest, Membership.getInstance().myGroup().name(),
				Membership.getInstance().myId());
	}

	public ReadRequest getReadRequest() {
		return (ReadRequest) serializable;
	}
	
}