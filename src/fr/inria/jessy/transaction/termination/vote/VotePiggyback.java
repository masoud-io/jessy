package fr.inria.jessy.transaction.termination.vote;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import fr.inria.jessy.ConstantPool;

/**
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class VotePiggyback implements Externalizable {
	private static final long serialVersionUID = -ConstantPool.JESSY_MID;

	Object piggyback;

	public VotePiggyback() {
	}

	public VotePiggyback(Object piggyback) {
		this.piggyback = piggyback;
	}

	public Object getPiggyback() {
		return piggyback;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		piggyback = in.readObject();

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(piggyback);
	}

}
