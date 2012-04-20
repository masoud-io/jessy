package fr.inria.jessy.store;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import fr.inria.jessy.ConstantPool;

public class ReadReply<E extends JessyEntity> implements Serializable {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	UUID readRequestId;
	Collection<E> entities;

	public ReadReply(E entity, UUID correspondingReadRequestId) {
		entities = new ArrayList<E>();
		this.entities.add(entity);
		this.readRequestId = correspondingReadRequestId;
	}

	public ReadReply(Collection<E> entities, UUID correspondingReadRequestId) {
		this.entities = entities;
		this.readRequestId = correspondingReadRequestId;
	}

	public UUID getReadRequestId() {
		return readRequestId;
	}

	public Collection<E> getEntity() {
		return entities;
	}

	public synchronized void mergeReply(ReadReply<E> readReply) {
		if (this.readRequestId.equals(readReply.getReadRequestId()))
			entities.addAll(readReply.getEntity());
	}

}
