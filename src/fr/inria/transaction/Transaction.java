package fr.inria.transaction;

import fr.inria.jessy.Jessy;

public abstract class Transaction {
	Jessy jessy;
	
	public Transaction(Jessy jessy){
		this.jessy=jessy;
	}
	
	public abstract boolean execute();

	public Jessy operation() {
		return jessy;
	}
	
	
}
