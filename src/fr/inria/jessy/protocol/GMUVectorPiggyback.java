package fr.inria.jessy.protocol;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.vector.GMUVector2;

/**
 * Used in the {@code Vote} for sending the new GMUVector to the write set of
 * the transaction.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class GMUVectorPiggyback implements Externalizable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	GMUVector2<String> vector;

	@Deprecated
	public GMUVectorPiggyback() {
	}

	public GMUVectorPiggyback(GMUVector2<String> vector) {
		this.vector = vector;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		vector = (GMUVector2<String>) in.readObject();

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(vector);
	}

}
