package fr.inria.jessy;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.CompactVector;

public interface JessyInterface {

	/**
	 * Performs a local or remote read operation depending on the specific
	 * implementation of Jessy.
	 * 
	 * @param <E>
	 *            Type of the entity to read the value from.
	 * @param <SK>
	 *            Type of the secondary key to read the value from.
	 * @param entityClass
	 *            Class of the entity to read the value from.
	 * @param keyName
	 *            The name of the secondary key
	 * @param keyValue
	 *            The value of the secondary key
	 * @param readList
	 *            List of vectors of already executed read operations.
	 * @return An entity with the secondary key equals keyName and its value
	 *         equals keyValue
	 */
	abstract <E extends JessyEntity, SK> E performRead(
			Class<E> entityClass, String keyName, SK keyValue,
			CompactVector<String> readSet) throws InterruptedException,
			ExecutionException;
	
	
	
	/**
	 * Performs a local or remote read operation depending on the specific
	 * implementation of Jessy on all provided keys.
	 * 
	 * @param <E>
	 *            Type of the entity to read the value from.
	 * @param <SK>
	 *            Type of the secondary key to read the value from.
	 * @param entityClass
	 *            Class of the entity to read the value from.
	 * @param keyName
	 *            The name of the secondary key
	 * @param keyValue
	 *            The value of the secondary key
	 * @param readList
	 *            List of vectors of already executed read operations.
	 * @return An entity with the secondary key equals keyName and its value
	 *         equals keyValue
	 */
	abstract <E extends JessyEntity> Collection<E> performRead(
			Class<E> entityClass, List<ReadRequestKey<?>> keys,
			CompactVector<String> readSet) throws InterruptedException,
			ExecutionException;
	
	/**
	 * Commit the open transaction, and garbage collect it.
	 * 
	 * @return
	 */
	abstract ExecutionHistory commitTransaction(
			TransactionHandler transactionHandler);

	abstract <E extends JessyEntity> void performNonTransactionalWrite(
			E entity) throws InterruptedException, ExecutionException;
	
	/**
	 * Apply changes of a writeSet and createSet of a committed transaction to
	 * the datastore.
	 * 
	 * @param transactionHandler
	 *            handler of a committed transaction.
	 */
	abstract void applyModifiedEntities(ExecutionHistory executionHistory);
}
