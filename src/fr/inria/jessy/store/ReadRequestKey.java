package fr.inria.jessy.store;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ReadRequestKey<K> implements Externalizable {

	private String keyName;
	private K keyValue;

	public ReadRequestKey(){
		
	}
	
	public ReadRequestKey(String keyName, K keyValue) {
		this.keyName = keyName;
		this.keyValue = keyValue;
	}

	public String getKeyName() {
		return keyName;
	}

	public K getKeyValue() {
		return keyValue;
	}

	@Override
	public void readExternal(ObjectInput arg0) throws IOException,
			ClassNotFoundException {
		keyName = (String) arg0.readObject();
		keyValue= (K) arg0.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput arg0) throws IOException {

		arg0.writeObject(keyName);
		arg0.writeObject(keyValue);
	}

}
