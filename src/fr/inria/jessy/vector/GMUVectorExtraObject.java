package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

public class GMUVectorExtraObject<K> implements Externalizable {
	
	GMUVector<K> snapshot;
	ArrayList<K> readProcesses;

	public GMUVectorExtraObject() {
		readProcesses=new ArrayList<K>();
	}

	public void addItem(GMUVector<K> pdv) {
		readProcesses.add(pdv.getSelfKey());
	}

	public ArrayList<K> getReadProcesses() {
		return readProcesses;
	}
	
	public GMUVector<K> getSnapshot() {
		return snapshot;
	}

	public void setSnapshot(GMUVector<K> snapshot) {
		this.snapshot = snapshot;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		readProcesses = (ArrayList<K>) in
				.readObject();
		snapshot=(GMUVector<K>) in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(readProcesses);
		out.writeObject(snapshot);
	}
}
