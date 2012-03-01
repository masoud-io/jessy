package fr.inria.jessy.transaction;

import java.util.concurrent.Callable;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.JessyEntity;

public abstract class Transaction implements Callable<ExecutionHistory>{

	private Jessy jessy;
	private TransactionHandler transactionHandler;

	public Transaction(Jessy jessy) throws Exception{
		this.jessy = jessy;
		this.transactionHandler = jessy.startTransaction();
	}

	public abstract ExecutionHistory execute();

	public <E extends JessyEntity> E read(Class<E> entityClass, String keyValue)
			throws Exception {
		return jessy.read(transactionHandler, entityClass, keyValue);
	}

	public <E extends JessyEntity> void write(E entity)
			throws NullPointerException {
		jessy.write(transactionHandler, entity);
	}

	public <E extends JessyEntity> void create(E entity) {
		jessy.create(transactionHandler, entity);
	}

	public ExecutionHistory commitTransaction() {
		return jessy.commitTransaction(transactionHandler);
	}

	public void abortTransaction(){
		jessy.abortTransaction(transactionHandler);
	}
	
	public ExecutionHistory call(){
		return execute();
	}

}
