package fr.inria.jessy.transaction.termination.message;

import java.util.Set;

import net.sourceforge.fractal.multicast.MulticastMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.termination.TerminationResult;

public class TerminateTransactionReplyMessage extends MulticastMessage {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	public TerminateTransactionReplyMessage() {
	}

	public TerminateTransactionReplyMessage(TerminationResult terminationResult, Set<String> dest, String gSource, int source) {
		super(terminationResult, dest, gSource ,source );
	}

	public TerminationResult getTerminationResult() {
		return (TerminationResult) serializable;
	}

}