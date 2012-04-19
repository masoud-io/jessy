package fr.inria.jessy;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.multicast.MulticastMessage;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

public class RemoteReadRequestMessage<E extends JessyEntity> extends MulticastMessage {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	private ReadRequest<E> request;
	
	// For Fractal
	public RemoteReadRequestMessage() {
	}

	public RemoteReadRequestMessage(ReadRequest<E> r) {
		super();
		request = r;
	}

	public ReadRequest<E> getReadRequest() {
		return request;
	}
	
	
	@Override
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws ClassNotFoundException, IOException{
		super.readExternal(in);
		request = (ReadRequest<E>) in.readObject();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException{
		super.writeExternal(out);
		out.writeObject(request);
	}
	
}