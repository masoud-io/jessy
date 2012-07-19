package fr.inria.jessy.communication.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;

import net.sourceforge.fractal.multicast.MulticastMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;


public class ReadReplyMessage<E extends JessyEntity> extends MulticastMessage {

	static final long serialVersionUID = ConstantPool.JESSY_MID;

	private List<ReadReply<E>> replies;
	
	// For Fractal
	public ReadReplyMessage(){
	}
	
	public ReadReplyMessage(List<ReadReply<E>> r) {
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
        source = in.readInt();
		swid = (String)in.readObject();
		dest = (Collection<String>) in.readObject();
		gSource = (String)in.readObject();
		replies = (List<ReadReply<E>>) in.readObject();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException{
        out.writeInt(source);
		out.writeObject(swid);
	    out.writeObject(dest);
	    out.writeObject(gSource);
		out.writeObject(replies);
	}
	
}