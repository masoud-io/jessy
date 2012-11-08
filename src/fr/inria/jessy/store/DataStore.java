package fr.inria.jessy.store;

import java.util.List;

import com.sleepycat.je.DatabaseException;

public interface DataStore {

	public void close() throws DatabaseException;

	public <E extends JessyEntity> void addPrimaryIndex(Class<E> entityClass)
			throws Exception;

	public <E extends JessyEntity, SK> void addSecondaryIndex(
			Class<E> entityClass, Class<SK> secondaryKeyClass,
			String secondaryKeyName) throws Exception;

	/**
	 * put the entity in the data store.
	 * 
	 * @param entity
	 *            entity to be put
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity> void put(E entity)
			throws NullPointerException;

	/**
	 * Retrieve the entity with {@link ReadRequest#getOneKey()} or
	 * {@link ReadRequest#getMultiKeys()}.
	 * 
	 * @param readRequest
	 *            requests that contains the keys.
	 * @return a read reply containing entities corresponds to the read request.
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity, SK> ReadReply<E> get(
			ReadRequest<E> readRequest) throws NullPointerException;

	/**
	 * Retrieve multiple entities with {@link ReadRequest#getOneKey()} or
	 * {@link ReadRequest#getMultiKeys()}.
	 * 
	 * @param readRequests
	 *            requests that contains multiple read requests, each containing
	 *            one or multi keys.
	 * @return
	 * @throws NullPointerException
	 */
	public <SK> List<ReadReply<JessyEntity>> getAll(
			List<ReadRequest<JessyEntity>> readRequests)
			throws NullPointerException;

	/**
	 * Delete the entity from the data store.
	 * 
	 * @param entityClassName
	 * @param secondaryKeyName
	 * @param keyValue
	 * @return
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity, SK> boolean delete(String entityClassName,
			String secondaryKeyName, SK keyValue) throws NullPointerException;

	/**
	 * Returns number of versions of the entity with the provided key.
	 * 
	 * @param entityClassName
	 * @param secondaryKeyName
	 * @param keyValue
	 * @return
	 * @throws NullPointerException
	 */
	public <E extends JessyEntity, SK, V> int getEntityCounts(
			String entityClassName, String secondaryKeyName, SK keyValue)
			throws NullPointerException;
}
