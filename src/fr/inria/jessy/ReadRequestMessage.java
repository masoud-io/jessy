package fr.inria.jessy;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import net.sourceforge.fractal.multicast.MulticastMessage;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

public class ReadRequestMessage extends MulticastMessage {

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
		super.readExternal(in);
		requests = (List<ReadRequest<JessyEntity>>) in.readObject();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException{
		super.writeExternal(out);
		out.writeObject(requests);
	}
	
	@Override
	public String toString(){
		return requests.toString();
	}
	
}