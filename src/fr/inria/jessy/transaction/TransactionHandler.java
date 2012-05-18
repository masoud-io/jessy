package fr.inria.jessy.transaction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.utils.CustomUUID;

public class TransactionHandler implements Externalizable{

	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	
	private  UUID id;
	private AtomicInteger transactionSeqNumber;
	
	public TransactionHandler(){
		this.id = CustomUUID.getNextUUID();
		this.transactionSeqNumber=Jessy.lastCommittedTransactionSeqNumber; 
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

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		id = (UUID) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(id);
	}
	
	public AtomicInteger getTransactionSeqNumber() {
		return transactionSeqNumber;
	}

	public void incrementTransactionSeqNumber() {
		transactionSeqNumber.incrementAndGet();
	}
	
}
