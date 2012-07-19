package fr.inria.jessy.store;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import fr.inria.jessy.ConstantPool;

/**
 * Any read request (either local or remote) should be sent to the data store
 * layer with an object of class {@code ReadRequestKey}
 * 
 * @author Masoud Saeida Ardekani
 * 
 * @param <K>
 */
public class ReadRequestKey<K> implements Externalizable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	
	/**
	 * In order to increase the marshaling and unmarshaling speed,
	 * {@code DEFAULT_KEY_NAME} is defined. If the key is the same as
	 * DEFAULT_KEY_NAME, it is not stored in {@code keyName} field. Thus, the
	 * cost of marshaling and unmarshaling is skipped.
	 */
	private final static String DEFAULT_KEY_NAME = "secondaryKey";

	private String keyName;
	private K keyValue;

	/**
	 * For Externalizable interface.
	 */
	@Deprecated
	public ReadRequestKey() {

	}

	public ReadRequestKey(String keyName, K keyValue) {
		if (keyName != DEFAULT_KEY_NAME)
			this.keyName = keyName;
		this.keyValue = keyValue;
	}

	public String getKeyName() {
		return (keyName != null) ? keyName : DEFAULT_KEY_NAME;
	}

	public K getKeyValue() {
		return keyValue;
	}

	@Override
	public void readExternal(ObjectInput arg0) throws IOException,
			ClassNotFoundException {
		keyName = (String) arg0.readObject();
		keyValue = (K) arg0.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput arg0) throws IOException {
		arg0.writeObject(keyName);
		arg0.writeObject(keyValue);
	}

	@Override
	public String toString(){
		return keyValue.toString();
	}
	
}
