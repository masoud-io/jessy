package fr.inria.jessy;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sun.nio.cs.HistoricallyNamedCharset;

import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.vector.Vector;

/**
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public abstract class Jessy {

	DataStore localDataStore;
	ExecutionHistory executionHistory;
	
	public Jessy(File environmentHome, boolean readOnly, String storeName)
			throws Exception {
		localDataStore = new DataStore(environmentHome, readOnly, storeName);
		
		executionHistory=new ExecutionHistory();
	}

	/**
	 * All entities that will be managed by Jessy middle ware should be introduced to it beforehand.
	 * Currently, it only supports to add one primary key and one secondary key for each entity.
	 * @param <E> Type of the entity that will be store/retrieve
	 * @param <SK> Type of the secondary key. This can usually be String
	 * @param entityClass Class of the entity that will be store/retrieve
	 * @param secondaryKeyClass Class of the secondary key. This can usually be String
	 * @param secondaryKeyName Name of the field that read request will be issued on.
	 * @throws Exception
	 */
	//TODO write a function for adding extra secondary keys!
	private <E extends JessyEntity, SK> void addEntity(Class<E> entityClass,
			Class<SK> secondaryKeyClass, String secondaryKeyName)
			throws Exception {
		localDataStore.addPrimaryIndex(entityClass);
		localDataStore.addSecondaryIndex(entityClass, secondaryKeyClass,
				secondaryKeyName);
		
		executionHistory.addEntity(entityClass);

	}
	
	public <E extends JessyEntity, SK> void addEntity(Class<E> entityClass)
			throws Exception {
		addEntity(entityClass, String.class, "secondaryKey");
	}

	/**
	 * Executes a read command on Jessy. It first checks to see
	 * if the transaction has written a value for the same object or not.
	 * If it has written a new value previously, it returns that value, 
	 * otherwise, it calls the {@link Jessy#performRead(Class, String, Object, List)}} method.
	 * <p>
	 * This read is performed on {@link JessyEntity#getSecondaryKey()}}}
	 * @param <E> Type of the object to read the value from.
	 * @param entityClass Class of the object to read the value from.
	 * @param keyValue The value of the secondary key  
	 * @return An entity with the secondary key value equals keyValue
	 */
	public <E extends JessyEntity> E read(Class<E> entityClass,
			String keyValue){

		E entity= executionHistory.getFromWriteSet(entityClass, keyValue);
		
		if (entity ==null){
			entity=performRead(entityClass, "secondaryKey", keyValue, executionHistory.getReadSetVector());
		}		
		
		if (entity!=null){
			executionHistory.addToReadSet(entity);
			return entity;			
		}
		else{
			return null;
		}
	}

	protected abstract <E extends JessyEntity, SK> E performRead(Class<E> entityClass,
			String keyName, SK keyValue, List<Vector<String>> readList);
	
	
	public <E extends JessyEntity> void write(E entity){
		executionHistory.addToWriteSet(entity);
	}

	public abstract <E extends JessyEntity> void create(E e);
	
	public abstract void startTransaction();

	public abstract boolean commitTransaction();

	public abstract boolean abortTransaction();	

}
