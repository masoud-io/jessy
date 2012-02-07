package fr.inria.jessy.store;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import static com.sleepycat.persist.model.Relationship.*;

import com.sleepycat.persist.model.SecondaryKey;

import fr.inria.jessy.store.JessyEntity;

/**
 * @author Masoud Saeida Ardekani
 * 
 */

@Entity
public class TestEntityClass extends JessyEntity{
	
	public TestEntityClass(){
		super();
	}


	@SecondaryKey(relate = MANY_TO_ONE)	
	private String entityID;

	private String data;

	/**
	 * @return the data
	 */
	public String getData() {
		return data;
	}


	/**
	 * @param data the data to set
	 */
	public void setData(String data) {
		this.data = data;
	}


	/**
	 * @return the entityID
	 */
	public String getEntityID() {
		return entityID;
	}


	/**
	 * @param entityID the entityID to set
	 */
	public void setEntityID(String entityID) {
		this.entityID = entityID;
	}


  

	

}
