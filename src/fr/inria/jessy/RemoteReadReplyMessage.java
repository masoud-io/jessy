package fr.inria.jessy;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import net.sourceforge.fractal.multicast.MulticastMessage;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;


public class RemoteReadReplyMessage<E extends JessyEntity> extends MulticastMessage {

	static final long serialVersionUID = ConstantPool.JESSY_MID;

	private List<ReadReply<E>> replies;
	
	// For Fractal
	public RemoteReadReplyMessage(){
	}
	
	public RemoteReadReplyMessage(List<ReadReply<E>> r) {
		super();
		replies = r;
	}


	public List<ReadReply<E>> getReadReplies() {
		return replies;
	}
	
	@Override
	public String toString() {
		return replies.toString();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws ClassNotFoundException, IOException{
		super.readExternal(in);
		replies = (List<ReadReply<E>>) in.readObject();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException{
		super.writeExternal(out);
		out.writeObject(replies);
	}

	
}