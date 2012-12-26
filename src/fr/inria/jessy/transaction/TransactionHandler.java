package fr.inria.jessy.transaction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.utils.CustomUUID;

public class TransactionHandler implements Externalizable, Cloneable{

	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	
	/**
	 * A unique id for identifying the previous aborted transaction either by voting or timeout.
	 * 
	 * 
	 * <p>
	 * When a transaction times out by the client or is aborted by a voting from a jessy replica, the client tries to
	 * re-execute the transaction again. In this case, it can happen that the
	 * second transaction is received in a jessy node, but it should wait in the
	 * queue, because the other transaction is not yet removed from the list.
	 * This variable helps us to check if the certification conflict is because
	 * of a dangling aborted transaction, or something else.
	 */
	private TransactionHandler previousAbortedTransactionHandler;
	
	private  UUID id;
	
	public TransactionHandler(){
		this.id = CustomUUID.getNextUUID(); 
	}

	public UUID getId() {
		return id;
	}

	public TransactionHandler getPreviousAbortedTransactionHandler() {
		return previousAbortedTransactionHandler;
	}

	public void setPreviousAbortedTransactionHandler(
			TransactionHandler previousAbortedTransactionHandler) {
		this.previousAbortedTransactionHandler = previousAbortedTransactionHandler;
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
		previousAbortedTransactionHandler=(TransactionHandler) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(id);
		if (previousAbortedTransactionHandler!=null)
			out.writeObject(previousAbortedTransactionHandler);
		else
			out.writeObject(null);
	}
	
	public TransactionHandler clone(){
		TransactionHandler result=null;
		try {
			result = (TransactionHandler) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		result.id=new UUID(this.id.getMostSignificantBits(), this.id.getLeastSignificantBits());
		
		return result;
	}
}
