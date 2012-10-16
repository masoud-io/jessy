package fr.inria.jessy.communication.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;

import org.jboss.netty.channel.Channel;

import net.sourceforge.fractal.multicast.MulticastMessage;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

public class ReadRequestMessage extends MulticastMessage {

	public transient Channel channel;
	
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	private List<ReadRequest<JessyEntity>> requests;
	
	// For Fractal
	public ReadRequestMessage() {
	}

	public ReadRequestMessage(List<ReadRequest<JessyEntity>> r) {
		super();
		requests = r;
	}

	public List<ReadRequest<JessyEntity>> getReadRequests() {
		return requests;
	}
	
	
	@Override
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws ClassNotFoundException, IOException{
        source = in.readInt();
		swid = (String)in.readObject();
		dest = (Collection<String>) in.readObject();
		gSource = (String)in.readObject();
		requests = (List<ReadRequest<JessyEntity>>) in.readObject();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException{
		out.writeInt(source);
		out.writeObject(swid);
		out.writeObject(dest);
		out.writeObject(gSource);
		out.writeObject(requests);
	}
	
	@Override
	public String toString(){
		return requests.toString();
	}
	
}