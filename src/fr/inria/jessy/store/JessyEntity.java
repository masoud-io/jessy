package fr.inria.jessy.store;

import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * @author Masoud Saeida Ardekani
 *
 * This class provides two uniform key for every entity defined inside JessyStore.
 * A primary key with Long type, and a secondary key with String type.
 * 
 */

@Persistent
public class JessyEntity {

	//TODO DEFINE VV here!
	
	@PrimaryKey(sequence="Jessy_Sequence")
	private Long primaryKey;
	
	
	public JessyEntity(){
	}
	
	/**
	 * @return the primaryKey
	 */
	public Long getPrimaryKey() {
		return primaryKey;
	}

	/**
	 * @param primaryKey the primaryKey to set
	 */
	public void setPrimaryKey(Long primaryKey) {
		this.primaryKey = primaryKey;
	}

}
