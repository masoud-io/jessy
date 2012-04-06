package fr.inria.jessy;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.vector.CompactVector;

//TODO Garabage collect upon commit

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
			String keyName, SK keyValue, CompactVector<String> readSet) {
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keyName,
				keyValue, readSet);
		Collection<E> col = getDataStore().get(readRequest).getEntity();

		if (col.iterator().hasNext())
			return col.iterator().next();
		else
			return null;
	}

	@Override
	protected <E extends JessyEntity> Collection<E> performRead(
			Class<E> entityClass, List<ReadRequestKey<?>> keys,
			CompactVector<String> readSet) throws InterruptedException,
			ExecutionException {
		ReadRequest<E> readRequest = new ReadRequest<E>(entityClass, keys,
				readSet);

		return getDataStore().get(readRequest).getEntity();

	}

 
	@Override
	public ExecutionHistory commitTransaction(
			TransactionHandler transactionHandler) {
		ExecutionHistory result = handler2executionHistory
				.get(transactionHandler);

		result.changeState(TransactionState.COMMITTING);

		if (consistency.certify(lastCommittedEntities,
				handler2executionHistory.get(transactionHandler))) {
			// certification test has returned true. we can commit.
			commitedTransactions.add(transactionHandler);
			applyWriteSet(transactionHandler);
			applyCreateSet(transactionHandler);
			result.changeState(TransactionState.COMMITTED);

		} else {
			result.changeState(TransactionState.ABORTED_BY_CERTIFICATION);

		}

		garbageCollectTransaction(transactionHandler);
		return result;

	}
	
	@Override
	public <E extends JessyEntity> void performNonTransactionalWrite(E entity) {
		dataStore.put(entity);

	}

}
