package fr.inria.jessy.communication.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.UUID;

import net.sourceforge.fractal.wanamcast.WanAMCastMessage;
import fr.inria.jessy.consistency.Consistency;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionTouchedKeys;

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

	private final Consistency consistency=ConsistencyFactory.getConsistencyInstance();

	TransactionTouchedKeys keys;

	public TransactionHandlerMessage(){

	}

	public TransactionHandlerMessage(ExecutionHistory eh, Collection<String> dest, String gSource, int source){
		super(eh.getTransactionHandler().getId().toString(), dest, gSource,source);
		keys=eh.getTransactionTouchedKeys();
	}	
	
	public boolean commute(WanAMCastMessage m){
		if(this==m) return true;
		return consistency.certificationCommute(keys, ((TransactionHandlerMessage) m).keys);
	}

	public UUID getId(){
		return UUID.fromString((String)serializable);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(keys);
	}


	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		super.readExternal(in);
		keys=(TransactionTouchedKeys) in.readObject();
	}

}
