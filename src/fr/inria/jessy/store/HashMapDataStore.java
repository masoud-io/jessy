package fr.inria.jessy.store;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.sleepycat.je.DatabaseException;

import fr.inria.jessy.persistence.FilePersistence;
import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VectorFactory;

/**
 * Implements a simple in-memory hashmap data store for jessy.
 * 
 * TODO 1 . Write to HDD upon calling close.
 * 
 * TODO 2 . implement {@link DataStore#get(ReadRequest)} and
 * {@link DataStore#getAll(List)} for multi-keys queries.
 * 
 * TODO 3. implement {@link DataStore#delete(String, String, Object)}
 * 
 * TODO 4. Garbage Collection
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class HashMapDataStore implements DataStore {

	ConcurrentHashMap<String, ArrayList> store;

	@SuppressWarnings("unchecked")
	public HashMapDataStore() {
		if (FilePersistence.loadFromDisk)
			store= (ConcurrentHashMap<String, ArrayList>) FilePersistence.readObject("HashMapDataStore.store");
		else 
			store = new ConcurrentHashMap<String, ArrayList>();
	}

	@Override
	public void close() throws DatabaseException {
		FilePersistence.writeObject(store, "HashMapDataStore.store");
	}

	@Override
	public <E extends JessyEntity> void addPrimaryIndex(Class<E> entityClass)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public <E extends JessyEntity, SK> void addSecondaryIndex(
			Class<E> entityClass, Class<SK> secondaryKeyClass,
			String secondaryKeyName) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public <E extends JessyEntity> void put(E entity)
			throws NullPointerException {

		if (store.containsKey(entity.getKey())) {
			store.get(entity.getKey()).add(entity);
		} else {
			ArrayList<E> tmp = new ArrayList<E>(1);
			tmp.add(entity);
			store.put(entity.getKey(), tmp);
		}
	}

	@Override
	public <E extends JessyEntity, SK> ReadReply<E> get(
			ReadRequest<E> readRequest) throws NullPointerException {

		if (!VectorFactory.prepareRead(readRequest)){
			E entity = null;
			return new ReadReply<E>(entity, readRequest.getReadRequestId());
		}
			
		CompactVector<String> readSet = readRequest.getReadSet();

		if (readRequest.isOneKeyRequest) {
			ArrayList<E> tmp = store.get(readRequest.getOneKey().getKeyValue()
					.toString());

			if (tmp == null) {
				throw new NullPointerException("Object with key "
						+ readRequest.getOneKey().getKeyValue()
						+ " does not exist in the Data Store.");
			}

			int index = tmp.size() - 1;
			E entity = (E) tmp.get(index--).clone();

			if (readSet == null) {
				return new ReadReply<E>(entity, readRequest.getReadRequestId());
			}

			while (entity != null) {
				Vector.CompatibleResult compatibleResult=entity.getLocalVector().isCompatible(readSet);
				
				
				if (compatibleResult == Vector.CompatibleResult.COMPATIBLE) {
					VectorFactory.postRead(readRequest, entity);
					return new ReadReply<E>(entity,							
							readRequest.getReadRequestId());
				} else {
					if (compatibleResult == Vector.CompatibleResult.NOT_COMPATIBLE_TRY_NEXT) {
						if (index == -1)
							break;
						entity = (E)tmp.get(index--).clone();
					} else {
						// NEVER_COMPATIBLE
						
						/*
						 * Instead of returning null to the client, and retry again, since we are sure that the decision can
						 * be made now, we call get again to return the correct version.
						 * Note that {@link Vector.CompatibleResult.NEVER_COMPATIBLE} is only used in the Snapshot Isolation consistency.
						 */
						return get(readRequest);
//						break;
					}
				}
			}

			entity = null;
			return new ReadReply<E>(entity, readRequest.getReadRequestId());
		}

		// TODO
		return null;
	}

	@Override
	public <SK> List<ReadReply<JessyEntity>> getAll(
			List<ReadRequest<JessyEntity>> readRequests)
			throws NullPointerException {

		List<ReadReply<JessyEntity>> result = new ArrayList<ReadReply<JessyEntity>>(
				1);
		for (ReadRequest<JessyEntity> rr : readRequests) {
			result.add(get(rr));
		}
		return result;
	}

	@Override
	public <E extends JessyEntity, SK> boolean delete(String entityClassName,
			String secondaryKeyName, SK keyValue) throws NullPointerException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <E extends JessyEntity, SK, V> int getEntityCounts(
			String entityClassName, String secondaryKeyName, SK keyValue)
			throws NullPointerException {
		return store.get(keyValue.toString()).size();
	}

}
