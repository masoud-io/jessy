package fr.inria.jessy;

import java.util.List;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.Vector;

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
		return getDataStore().get(entityClass, keyName, keyValue, readSet);
	}

	@Override
	public synchronized boolean terminateTransaction(
			TransactionHandler transactionHandler) {

		return consistency.certify(lastCommittedEntities,
				handler2executionHistory.get(transactionHandler));
	}

}
