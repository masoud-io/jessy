package fr.inria.jessy.store;

import java.io.Serializable;
import java.util.UUID;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.vector.CompactVector;

public class ReadRequest<E extends JessyEntity, SK> implements Serializable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	
	Class<E> entityClass;
	CompactVector<String> readSet;
	SK keyvalue;
	String keyName;
	UUID readRequestId;
	

	public ReadRequest(Class<E> entityClass, String keyName, SK keyValue,
			CompactVector<String> readSet) {
		this.entityClass = entityClass;
		this.readSet = readSet;
		this.keyvalue=keyValue;
		this.keyName=keyName;
		readRequestId=UUID.randomUUID();
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

	public String getKeyName() {
		return keyName;
	}

	public UUID getReadRequestId() {
		return readRequestId;
	}
	
	/**
	 * This method returns the key that is used to partition entities across different processes.
	 * @return
	 */
	public String getPartitioningKey(){
		return this.entityClass.toString() + this.keyvalue.toString();
	}
	
}
