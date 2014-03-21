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
 * This class implements a generic and externalizable interface. It is used
 * during read requests for sending requests over the network.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class CompactVector<K> extends ValueVector<K, Integer> implements
		Externalizable {

	private static final long serialVersionUID = -ConstantPool.JESSY_MID;;

	private final static Integer _bydefault;

	private List<K> keys;

	/**
	 * if true, then initialize and serialize the
	 * {@code CompactVector#extraObject}, otherwise it skips the extraObject.
	 */
	private static boolean requireExtraObject;

	/**
	 * an extra object that might be used for storing extra data.
	 * 
	 * e.g., in GMUVector implementation, we stores hasRead hashmap.
	 */
	private ExtraObjectContainer extraObjectContainer;

	static {
		requireExtraObject = VectorFactory.needExtraObject();
		_bydefault = -1;
	}

	@Deprecated
	public CompactVector() {
		super(_bydefault);
		keys = new ArrayList<K>(1);
		extraObjectContainer=new ExtraObjectContainer();

	}

	public void update(Vector<K> entityLocalvector, Object entityTemproryObject) {
		if (requireExtraObject){
			entityLocalvector.updateExtraObjectInCompactVector(entityLocalvector,entityTemproryObject, extraObjectContainer);
		}
		super.update(entityLocalvector);
		keys.add(entityLocalvector.getSelfKey());
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

	public Object getExtraObject() {
		return extraObjectContainer.extraObject;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(keys);
		if (requireExtraObject) {
			out.writeObject(extraObjectContainer);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		super.setBydefault(_bydefault);
		keys = (List<K>) in.readObject();
		if (requireExtraObject) {
			extraObjectContainer = (ExtraObjectContainer) in.readObject();
		}
	}

}
