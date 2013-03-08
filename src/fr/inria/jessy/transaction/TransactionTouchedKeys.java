package fr.inria.jessy.transaction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

public class TransactionTouchedKeys implements Externalizable{

	public ArrayList<String> readKeys;
	public ArrayList<String> writeKeys;
	public ArrayList<String> createKeys;
	
	
	public TransactionTouchedKeys(){
		
	}

	public TransactionTouchedKeys(ArrayList<String> readKeys,
			ArrayList<String> writeKeys, ArrayList<String> createKeys) {
		this.readKeys = readKeys;
		this.writeKeys = writeKeys;
		this.createKeys = createKeys;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(readKeys);
		out.writeObject(writeKeys);
		out.writeObject(createKeys);
	}


	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		readKeys=(ArrayList<String>) in.readObject();
		writeKeys=(ArrayList<String>) in.readObject();
		createKeys=(ArrayList<String>) in.readObject();
	}
}
