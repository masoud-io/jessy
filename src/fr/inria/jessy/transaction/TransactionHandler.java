package fr.inria.jessy.transaction;

import java.io.Serializable;
import java.util.UUID;

import fr.inria.jessy.ConstantPool;

public class TransactionHandler implements Serializable{

	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	
	private  UUID id;
	
	public TransactionHandler(){
		this.id=UUID.randomUUID();
	}

	public UUID getId() {
		return id;
	}
	
	
}
