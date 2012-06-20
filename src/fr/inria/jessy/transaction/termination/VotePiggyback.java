package fr.inria.jessy.transaction.termination;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class VotePiggyback implements Externalizable {
	Object piggyback;

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
