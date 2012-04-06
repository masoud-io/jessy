package fr.inria.jessy.transaction.termination.message;

import java.util.Set;

import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.wanamcast.WanAMCastMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory;

public class TerminateTransactionRequestMessage extends WanAMCastMessage {
	
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	public TerminateTransactionRequestMessage() {
	}

	public TerminateTransactionRequestMessage(ExecutionHistory eh,
			Set<String> dest, String myGroup) {
		super(eh, dest, myGroup, Membership.getInstance().myId());
	}

	public ExecutionHistory getExecutionHistory() {
		return (ExecutionHistory) serializable;
	}

}
