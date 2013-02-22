package fr.inria.jessy.communication.message;

import java.util.Collection;
import java.util.UUID;

import net.sourceforge.fractal.wanamcast.WanAMCastMessage;

/**
 * This class is for performance improvement.
 * Since the size of {@link TerminateTransactionRequestMessage} is huge,
 * instead of am-casting it, we simply reliable multicast it, and only am-cast {@link this}.
 * This, all the consensus in am-cast are done with this message, instead of being done
 * with {@link TerminateTransactionRequestMessage}.
 * @author Masoud Saeida Ardekani
 *
 */
public class TransactionHandlerMessage extends WanAMCastMessage{

	public TransactionHandlerMessage(){
		
	}
	
	public TransactionHandlerMessage(UUID id, Collection<String> dest, String gSource, int source){
		super(id, dest, gSource,source);
	}
	
	public UUID getId(){
		return (UUID)serializable;
	}

}
