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
import java.util.UUID;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.ReadRequestKey;

/**
 * @author Masoud Saeida Ardekani
 * 
 *         This class implements a generic and serializable interface. It is
 *         used during read requests, and since it is compact, it is perfect for
 *         sending over the network
 */
public class CompactVector<K> extends ValueVector<K, Integer> implements
		Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;;

	private List<K> keys;

	public CompactVector() {
		super(-1);
		keys = new ArrayList<K>();
	}
	
	public void update(Vector<K> vector){
		super.update(vector);
		keys.add(vector.getSelfKey());
	}

	public List<K> getKeys(){
		return keys;
	}
	
	public Integer getValue(K key){
		return super.getValue(key);
	}
	
	public int size(){
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
		keys = (List<K>) in.readObject();
	}

}
