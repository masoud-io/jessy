package fr.inria.jessy.store;

import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;

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

	public JessyEntity(Vector<String> vector) {
		localVector = vector;
	}

	@PrimaryKey(sequence = "Jessy_Sequence")
	private Long primaryKey;

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

	public Vector<String> getLocalVector() {
		return localVector;
	}

	public abstract <T> String getLocalVectorSelfKey(T entityID);

}
