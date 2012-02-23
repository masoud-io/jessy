package fr.inria.transaction;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.JessyEntity;

public abstract class Transaction {
	
	Jessy jessy;
	TransactionHandler transactionHandler;

	public Transaction(Jessy jessy, TransactionHandler transactionHandler) {
		this.jessy = jessy;
		this.transactionHandler=transactionHandler;
	}

	public abstract boolean execute();

	public Jessy getJessy() {
		return jessy;
	}

	public <E extends JessyEntity> E read(Class<E> entityClass, String keyValue)
			throws Exception {
		return jessy.read(transactionHandler, entityClass, keyValue);
	}
}
