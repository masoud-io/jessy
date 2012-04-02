package fr.inria.jessy.transaction.termination.message;

import java.util.Set;

import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.rmcast.WanMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.termination.TerminationResult;

public class TerminateTransactionReplyMessage extends WanMessage {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	public TerminateTransactionReplyMessage() {
	}

	public TerminateTransactionReplyMessage(
			TerminationResult terminationResult, Set<String> dest,String myGroup) {
		super(terminationResult, dest, myGroup, Membership.getInstance()
				.myId());
	}

	public TerminationResult getTerminationResult() {
		return (TerminationResult) serializable;
	}

}