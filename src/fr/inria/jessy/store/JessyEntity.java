package fr.inria.jessy.store;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

import fr.inria.jessy.vector.Vector;

/**
 * @author Masoud Saeida Ardekani
 * 
 *         This class provides one uniform key for every entity defined inside
 *         JessyStore. A primary key with Long type
 * 
 */

@Persistent
public abstract class JessyEntity {

	private Vector<String> localVector;

	public JessyEntity(){
		
	}
	
	public JessyEntity(Vector<String> vector) {
		localVector = vector;
	}

	@PrimaryKey(sequence = "Jessy_Sequence")
	private Long primaryKey;

	@SecondaryKey(relate = MANY_TO_ONE)	
	private String secondaryKey;
	
	/**
	 * @return the primaryKey
	 */
	public Long getPrimaryKey() {
		return primaryKey;
	}

	/**
	 * @param primaryKey
	 *            the primaryKey to set
	 */
	public void setPrimaryKey(Long primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	

	public String getSecondaryKey() {
		return secondaryKey;
	}

	public void setSecondaryKey(String secondaryKey) {
		this.secondaryKey = secondaryKey;
	}

	public Vector<String> getLocalVector() {
		return localVector;
	}

	public abstract <T> String getLocalVectorSelfKey(T entityID);
	
	public abstract String getUniqueName();

}
