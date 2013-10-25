package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

/**
 * Holds extra information required for performing
 * {@link Vector#isCompatible(CompactVector)}
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class PDVExtraObject<K> implements Externalizable {

	PartitionDependenceVector<K> snapshot;
	ArrayList<PartitionDependenceVector<K>> readItems;

	public PDVExtraObject() {
		readItems = new ArrayList<PartitionDependenceVector<K>>();
	}

	public void addItem(PartitionDependenceVector<K> pdv) {
		readItems.add(pdv);
	}

	public ArrayList<PartitionDependenceVector<K>> getReadItems() {
		return readItems;
	}
	
	public PartitionDependenceVector<K> getSnapshot() {
		return snapshot;
	}

	public void setSnapshot(PartitionDependenceVector<K> snapshot) {
		this.snapshot = snapshot;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		readItems = (ArrayList<PartitionDependenceVector<K>>) in
				.readObject();
		snapshot=(PartitionDependenceVector<K>) in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(readItems);
		out.writeObject(snapshot);
	}

}
