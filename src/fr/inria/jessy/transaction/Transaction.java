package fr.inria.jessy.transaction;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequestKey;

//FIXME COMMENT ME PLZ!
public abstract class Transaction implements Callable<ExecutionHistory> {

	private Jessy jessy;
	private TransactionHandler transactionHandler;

	public Transaction(Jessy jessy) throws Exception {
		this.jessy = jessy;
		this.transactionHandler = jessy.startTransaction();
	}

	public abstract ExecutionHistory execute();

	public <E extends JessyEntity> E read(Class<E> entityClass, String keyValue)
			throws Exception {
		E entity = jessy.read(transactionHandler, entityClass, keyValue);
		if (entity != null)
			entity.setPrimaryKey(null);
		return entity;
	}

	public <E extends JessyEntity, SK> E read(Class<E> entityClass,
			String keyName, SK keyValue) throws Exception {
		return jessy.read(transactionHandler, entityClass, keyName, keyValue);
	}

	public <E extends JessyEntity> Collection<E> read(Class<E> entityClass,
			List<ReadRequestKey<?>> keys) throws Exception {
		return jessy.read(transactionHandler, entityClass, keys);
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

	public ExecutionHistory abortTransaction() {
		return jessy.abortTransaction(transactionHandler);
	}

	public ExecutionHistory call() {
		return execute();
	}

}
