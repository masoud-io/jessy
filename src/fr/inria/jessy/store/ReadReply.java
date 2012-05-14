package fr.inria.jessy.store;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import fr.inria.jessy.ConstantPool;

public class ReadReply<E extends JessyEntity> implements Externalizable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	int readRequestId;
	Collection<E> entities;

	/**
	 * For externalizable interface
	 */
	@Deprecated
	public ReadReply() {

	}

	public ReadReply(E entity, int correspondingReadRequestId) {
		entities = new ArrayList<E>();
		this.entities.add(entity);
		this.readRequestId = correspondingReadRequestId;
	}

	public ReadReply(Collection<E> entities, int correspondingReadRequestId) {
		this.entities = entities;
		this.readRequestId = correspondingReadRequestId;
	}

	public int getReadRequestId() {
		return readRequestId;
	}

	public Collection<E> getEntity() {
		return entities;
	}

	public synchronized void mergeReply(ReadReply<E> readReply) {
		if (this.readRequestId!=readReply.getReadRequestId())
			throw new IllegalArgumentException("Invalid requestId");
		entities.addAll(readReply.getEntity());
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(readRequestId);
//		 out.writeObject(entities);
		if (entities.size() == 1) {
			out.writeBoolean(true);
			out.writeObject(entities.iterator().next());
		} else {
			out.writeBoolean(false);
			out.writeObject(entities);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		readRequestId = in.readInt();
//		 entities = (Collection<E>) in.readObject();
		if (in.readBoolean()) {
			entities = new ArrayList<E>();
			entities.add((E) in.readObject());
		} else {
			entities = (Collection<E>) in.readObject();
		}
	}

}
