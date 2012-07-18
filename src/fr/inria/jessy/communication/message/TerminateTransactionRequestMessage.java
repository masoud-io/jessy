package fr.inria.jessy.communication.message;

import java.util.Collection;

import net.sourceforge.fractal.wanamcast.WanAMCastMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.ExecutionHistory;

public class TerminateTransactionRequestMessage extends WanAMCastMessage {
	
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	// For Fractal
	public TerminateTransactionRequestMessage() {
	}

	public TerminateTransactionRequestMessage(ExecutionHistory eh, Collection<String> dest, String gSource, int source) {
		super(eh, dest, gSource, source);
	}

	public ExecutionHistory getExecutionHistory() {
		return (ExecutionHistory) serializable;
	}

}
