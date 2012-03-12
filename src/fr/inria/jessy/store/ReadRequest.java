package fr.inria.jessy.store;

import java.io.Serializable;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.vector.CompactVector;

public class ReadRequest<SK> implements Serializable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	Class<?> entityClass;
	String secondaryKeyName;
	CompactVector<String> readSet;
	Object keyValue;
	

	<E extends JessyEntity, SK> ReadRequest(Class<E> entityClass,
			String secondaryKeyName, SK keyValue, CompactVector<String> readSet) {
		this.entityClass = entityClass;
		this.secondaryKeyName = secondaryKeyName;
		this.readSet = readSet;
		this.keyValue=keyValue;
	}

}
