package fr.inria.jessy;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.Message;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;


public class RemoteReadReplyMessage<E extends JessyEntity> extends Message {

	static final long serialVersionUID = ConstantPool.JESSY_MID;

	private ReadReply<E> reply;
	
	// For Fractal
	public RemoteReadReplyMessage() {
	}

	public RemoteReadReplyMessage(ReadReply<E> r) {
		super();
		reply=r;
	}

	public ReadReply<E> getReadReply() {
		return reply;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws ClassNotFoundException, IOException{
		reply = (ReadReply<E>) in.readObject();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException{
		out.writeObject(reply);
	}

	
}