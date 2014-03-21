package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class VersionVectorExtraObject<K> implements Externalizable {
	
	VersionVector<K> snapshot;
	
	public VersionVector<K> getSnapshot() {
		return snapshot;
	}

	public void setSnapshot(VersionVector<K> snapshot) {
		this.snapshot = snapshot;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		snapshot=(VersionVector<K>) in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(snapshot);
	}
}
