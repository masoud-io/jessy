package fr.inria.jessy.store;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.vector.CompactVector;

public class ReadRequest<E extends JessyEntity, SK> implements Serializable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	Class<E> entityClass;
	CompactVector<String> readSet;
	SK keyvalue;
	String secondaryKeyName;
	
	public ReadRequest(Class<E> entityClass, String secondaryKeyName, SK keyValue,
			CompactVector<String> readSet) {
		this.entityClass = entityClass;
		this.readSet = readSet;
		this.keyvalue=keyValue;
		this.secondaryKeyName=secondaryKeyName;

	}

	public Class<E> getEntityClass() {
		return entityClass;
	}

	public CompactVector<String> getReadSet() {
		return readSet;
	}

	public SK getKeyvalue() {
		return keyvalue;
	}

	public String getSecondaryKeyName() {
		return secondaryKeyName;
	}

	
}
