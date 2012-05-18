/**
 * 
 */
package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import fr.inria.jessy.ConstantPool;

/**
 * This class implements a generic and externalizable interface. It is used during
 * read requests for sending requests over the network.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class CompactVector<K> extends ValueVector<K, Integer> implements
		Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;;
	private final static Integer _bydefault = -1;

	private List<K> keys;

	public CompactVector() {
		super(_bydefault);
		keys = new ArrayList<K>();
	}

	public void update(Vector<K> vector) {
		super.update(vector);
		keys.add(vector.getSelfKey());
	}

	public List<K> getKeys() {
		return keys;
	}

	public Integer getValue(K key) {
		return super.getValue(key);
	}

	public int size() {
		return keys.size();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(keys);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		super.setBydefault(_bydefault);
		keys = (List<K>) in.readObject();
	}

}
