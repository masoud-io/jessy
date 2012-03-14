package fr.inria.jessy.store;

import java.io.Serializable;
import java.util.UUID;

import fr.inria.jessy.ConstantPool;

public class ReadReply<E extends JessyEntity> implements Serializable {
	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	UUID readRequestId;
	E entity;

	public ReadReply(E entity, UUID correspondingReadRequestId) {
		this.entity = entity;
		this.readRequestId = correspondingReadRequestId;
	}

	public UUID getReadRequestId() {
		return readRequestId;
	}

	public E getEntity() {
		return entity;
	}
	
	
}
