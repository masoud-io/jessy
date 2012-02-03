package fr.inria.jessy;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import static com.sleepycat.persist.model.Relationship.*;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * @author Masoud Saeida Ardekani
 * 
 */

@Entity
public class EntClass {

	public EntClass(){
		
	}
	public EntClass(String pKey, String sKey, String classValue) {
		this.pKey = pKey;
		this.sKey = sKey;
		this.classValue = classValue;
	}

	@PrimaryKey
	private String pKey;

	@SecondaryKey(relate = MANY_TO_ONE)
	private String sKey;

	private String classValue;

	/**
	 * @return the pKey
	 */
	public String getpKey() {
		return pKey;
	}

	/**
	 * @param pKey
	 *            the pKey to set
	 */
	public void setpKey(String pKey) {
		this.pKey = pKey;
	}

	/**
	 * @return the sKey
	 */
	public String getsKey() {
		return sKey;
	}

	/**
	 * @param sKey
	 *            the sKey to set
	 */
	public void setsKey(String sKey) {
		this.sKey = sKey;
	}

	/**
	 * @return the classValue
	 */
	public String getClassValue() {
		return classValue;
	}

	/**
	 * @param classValue
	 *            the classValue to set
	 */
	public void setClassValue(String classValue) {
		this.classValue = classValue;
	}

}
