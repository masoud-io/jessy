package fr.inria.jessy.store;

import java.io.Serializable;

import fr.inria.jessy.ConstantPool;

public class ReadRequestKey<K> implements Serializable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	private String keyName;
	private K keyValue;

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
	
	
}
