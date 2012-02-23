package fr.inria.jessy.transaction;

public class TransactionHandler {

	int id;
	
	public TransactionHandler(int id){
		this.id=id;
	}

	protected int getId() {
		return id;
	}
	
	
}
