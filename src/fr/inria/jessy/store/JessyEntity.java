package fr.inria.jessy.store;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VectorFactory;

/**
 * @author Masoud Saeida Ardekani
 * 
 *         This class provides two keys to every object stored by {@link Jessy}}
 *         PrimaryKey is of type {@link Long}
 *         SecondaryKey is of type {@link String}
 */

@Persistent
public abstract class JessyEntity {
	
	
	private Vector<String> localVector;

	private boolean removoed=false;
	
	public boolean isRemovoed() {
		return removoed;
	}

	/**
	 * Set the entity to be removed by the garbage collector.
	 * TODO set localVector to null to save memory! (safety might be broken)
	 * 
	 */
	public void removoe() {
		this.removoed = true;
	}

	public JessyEntity(String entityClassName, String entityId) {
		localVector = VectorFactory.getVector(entityClassName + entityId);
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
	
	public void setLocalVector(Vector<String> localVector){
		this.localVector=localVector;
	}

	public abstract String getKey();
	
}
