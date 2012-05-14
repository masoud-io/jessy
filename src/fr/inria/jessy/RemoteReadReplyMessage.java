package fr.inria.jessy;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.multicast.MulticastMessage;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;


public class RemoteReadReplyMessage<E extends JessyEntity> extends MulticastMessage {

	static final long serialVersionUID = ConstantPool.JESSY_MID;
	private static TimeRecorder unpackTime = new TimeRecorder("RemoteReadReplyMessage#unpackTime");

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
		super.readExternal(in);
		unpackTime.start();
		reply = (ReadReply<E>) in.readObject();
		unpackTime.stop();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException{
		super.writeExternal(out);
		out.writeObject(reply);
	}

	
}