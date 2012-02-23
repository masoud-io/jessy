package fr.inria.jessy;

import java.util.List;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.Vector;

public class LocalJessy extends Jessy {

	private static LocalJessy localJessy = null;

	private LocalJessy() throws Exception {
		super();
	}

	public static synchronized LocalJessy getInstance() throws Exception {
		if (localJessy == null) {
			localJessy = new LocalJessy();
		}
		return localJessy;
	}

	@Override
	protected <E extends JessyEntity, SK> E performRead(Class<E> entityClass,
			String keyName, SK keyValue, List<Vector<String>> readList) {
		return getDataStore().get(entityClass, keyName, keyValue, readList);
	}

	@Override
	public <E extends JessyEntity> void create(
			TransactionHandler transactionHandler, E entity) {
		write(transactionHandler, entity);
	}

	@Override
	public synchronized boolean commitTransaction(
			TransactionHandler transactionHandler) {
		boolean result = false;

		result=canCommit(transactionHandler);
		
		if (result == true)
			getCommitedTransactions().add(transactionHandler);
		return result;
	}

	@Override
	public void abortTransaction(TransactionHandler transactionHandler) {
		getAbortedTransactions().add(transactionHandler);
		// TODO re-execute the transaction
	}

	@Override
	protected boolean canCommit(TransactionHandler transactionHandler) {
		// TODO Check to see whether the transaction can commit locally or not.
		return false;
	}

}
