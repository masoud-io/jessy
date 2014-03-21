package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This class is a simple container for the extra object that is passed by {@link CompactVector}
 *  
 * @author Masoud Saeida Ardekani
 *
 */
public class ExtraObjectContainer implements Externalizable  {

	public Object extraObject;

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(extraObject);
		
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		extraObject=in.readObject();
	}
}
