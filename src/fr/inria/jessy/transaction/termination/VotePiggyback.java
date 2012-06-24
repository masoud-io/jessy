package fr.inria.jessy.transaction.termination;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import fr.inria.jessy.consistency.ParallelSnapshotIsolationPiggyback;


/**
 * 
 * @author Masoud Saeida Ardekani
 * 
 *         TODO Generalize this class
 * 
 */
public class VotePiggyback implements Externalizable {
	ParallelSnapshotIsolationPiggyback piggyback;

	public VotePiggyback() {
	}

	public VotePiggyback(ParallelSnapshotIsolationPiggyback piggyback) {
		this.piggyback = piggyback;
	}

	public ParallelSnapshotIsolationPiggyback getPiggyback() {
		return piggyback;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		piggyback = (ParallelSnapshotIsolationPiggyback) in.readObject();

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {

		out.writeObject(piggyback);
	}

}
