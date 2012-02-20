package fr.inria.jessy;

import java.io.File;

import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;

/**
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class Jessy {

	DataStore localDataStore;

	public Jessy(File environmentHome, boolean readOnly, String storeName) throws Exception{
		localDataStore = new DataStore(environmentHome, readOnly, storeName);
	}
	
	public <E extends JessyEntity, SK> void addEntity(Class<E> entityClass, Class<SK> secondaryKeyClass,
			String secondaryKeyName) throws Exception{
		localDataStore.addPrimaryIndex(entityClass);
		localDataStore.addSecondaryIndex(entityClass, secondaryKeyClass, secondaryKeyName);
	}
	
	public void startTransaction(){
		
	}
	
	

}
