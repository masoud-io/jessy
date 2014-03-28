package fr.inria.jessy.protocol;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.transaction.TransactionHandler;

/**
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class SDURPiggyback implements Externalizable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;
	

	private Integer SC;
	
	private String groupName;

	private TransactionHandler transactionHandler; 
	
	@Deprecated
	public SDURPiggyback() {
	}

	public SDURPiggyback(Integer SC, TransactionHandler handler, String groupName) {
		this.SC = SC;
		this.transactionHandler=handler;
		this.groupName=groupName;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		SC = (Integer) in.readObject();
		transactionHandler=(TransactionHandler)in.readObject();
		groupName=(String)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(SC);
		out.writeObject(transactionHandler);
		out.writeObject(groupName);
	}

	public Integer getSC() {
		return SC;
	}

	public TransactionHandler getTransactionHandler() {
		return transactionHandler;
	}
	
	public String getGroupName(){
		return groupName;
	}

	@Override
	public int hashCode(){
		return groupName.hashCode();
	}
	
	@Override
	public boolean equals(Object obj){
		return ((SDURPiggyback)obj).groupName.equals(this.groupName);
	}
	
}
