package fr.inria.jessy.transaction.termination.message;

import java.util.Set;

import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.rmcast.WanMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.termination.Vote;

public class VoteMessage extends WanMessage {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	public VoteMessage() {
	}

	public VoteMessage(Vote vote, Set<String> dest) {
		super(vote, dest, Membership.getInstance().myGroup().name(),
				Membership.getInstance().myId());
	}

	public Vote getVote() {
		return (Vote) serializable;
	}

}
