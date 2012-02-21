package fr.inria.jessy;

import java.io.File;

import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;

/**
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public abstract class Jessy {

	DataStore localDataStore;
		

	public Jessy(File environmentHome, boolean readOnly, String storeName) throws Exception{
		localDataStore = new DataStore(environmentHome, readOnly, storeName);
	}
	
	public <E extends JessyEntity, SK> void addEntity(Class<E> entityClass, Class<SK> secondaryKeyClass,
			String secondaryKeyName) throws Exception{
		localDataStore.addPrimaryIndex(entityClass);
		localDataStore.addSecondaryIndex(entityClass, secondaryKeyClass, secondaryKeyName);
	}
	
	public abstract void startTransaction();
	
	public abstract boolean commitTransaction();
	
	public abstract boolean abortTransaction();
		
	public abstract <E extends JessyEntity, SK> E read(SK k);
	
	public abstract <E extends JessyEntity> void write(E e);	
	
	public abstract <E extends JessyEntity> void create(E e);


}
