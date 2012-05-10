package fr.inria.jessy.store;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.utils.CustomUUID;
import fr.inria.jessy.vector.CompactVector;

//TODO Comment me and all methods
public class ReadRequest<E extends JessyEntity> implements Externalizable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	private static CustomUUID customUUID=new CustomUUID();

	String entityClassName;
	CompactVector<String> readSet;
	List<ReadRequestKey<?>> keys;
	UUID readRequestId;

	/**
	 * For externalizable interface
	 */
	@Deprecated
	public ReadRequest() {

	}

	/**
	 * This constructor should be called if the {@code ReadRequest} is only on
	 * one {@code ReadRequestKey}
	 * 
	 * @param <K>
	 * @param entityClass
	 * @param keyName
	 * @param keyValue
	 * @param readSet
	 * @param partitioningKey
	 */
	public <K> ReadRequest(Class<E> entityClass, String keyName, K keyValue,
			CompactVector<String> readSet) {
		this.entityClassName = entityClass.getName();
		this.readSet = readSet;

		keys = new ArrayList<ReadRequestKey<?>>();
		ReadRequestKey<K> key = new ReadRequestKey<K>(keyName, keyValue);
		keys.add(key);		
		readRequestId = customUUID.getNextUUID();
	}

	/**
	 * This constructor should be called if the {@code ReadRequest} is only on
	 * several {@code ReadRequestKey}
	 * 
	 * @param entityClass
	 * @param keys
	 * @param readSet
	 * @param partitioningKey
	 */
	public ReadRequest(Class<E> entityClass, List<ReadRequestKey<?>> keys,
			CompactVector<String> readSet) {
		this.entityClassName = entityClass.getName();
		this.readSet = readSet;

		this.keys = keys;
		readRequestId = UUID.randomUUID();
	}

	public String getEntityClassName() {
		return entityClassName;
	}

	public CompactVector<String> getReadSet() {
		return readSet;
	}

	public UUID getReadRequestId() {
		return readRequestId;
	}

	public List<ReadRequestKey<?>> getKeys() {
		return keys;
	}

	/**
	 * FIXME consider the first key as the partitioning key. Is it correct? This
	 * method returns the key that is used to partition entities across
	 * different processes.
	 * 
	 * @return
	 */
	public String getPartitioningKey() {
		return this.keys.get(0).getKeyValue().toString();
	}

	@Override
	public String toString() {
		return getReadRequestId().toString();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(entityClassName);
		out.writeObject(readSet);
		out.writeObject(keys);
		out.writeObject(readRequestId);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {

		entityClassName = (String) in.readObject();
		readSet = (CompactVector<String>) in.readObject();
		keys = (List<ReadRequestKey<?>>) in.readObject();
		readRequestId = (UUID) in.readObject();
	}

}
