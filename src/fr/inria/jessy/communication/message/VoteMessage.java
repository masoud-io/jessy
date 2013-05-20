package fr.inria.jessy.communication.message;

import java.util.Set;

import net.sourceforge.fractal.multicast.MulticastMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.termination.vote.Vote;

public class VoteMessage extends MulticastMessage {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	public VoteMessage() {
	}

	public VoteMessage(Vote vote, Set<String> dest, String gsource, int source) {
		super(vote, dest, gsource, source);
	}

	public Vote getVote() {
		return (Vote) serializable;
	}

	public String toString(){
		return serializable.toString();
	}
	
}
