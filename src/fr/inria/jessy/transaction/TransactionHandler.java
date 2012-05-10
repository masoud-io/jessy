package fr.inria.jessy.transaction;

import java.io.Serializable;
import java.util.UUID;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.utils.CustomUUID;

public class TransactionHandler implements Serializable{

	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	
	private static CustomUUID customUUID;
	
	private  UUID id;
	
	public TransactionHandler(){
		this.id=customUUID.getNextUUID();
	}

	public UUID getId() {
		return id;
	}

	@Override
	public int hashCode(){
		return id.hashCode();
	}
	
	@Override
	public boolean equals(Object obj){
		if ( this == obj ) return true;
		if ( !(obj instanceof TransactionHandler) ) return false;
		return id.equals(((TransactionHandler)obj).id);
	}
	
	@Override
	public String toString(){
		return id.toString();
	}
	
}
