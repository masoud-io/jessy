package fr.inria.jessy;

import java.util.Set;

import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.rmcast.WanMessage;
import fr.inria.jessy.store.ReadReply;


public class RemoteReadReplyMessage extends WanMessage {

	static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	public RemoteReadReplyMessage() {
	}

	public RemoteReadReplyMessage(ReadReply r, Set<String> d) {
		super(r, d, Membership.getInstance().myGroup().name(),
				Membership.getInstance().myId());
	}

	public ReadReply getReadReply() {
		return (ReadReply) serializable;
	}

}