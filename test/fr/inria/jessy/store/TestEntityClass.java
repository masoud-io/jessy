package fr.inria.jessy.store;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import static com.sleepycat.persist.model.Relationship.*;

import com.sleepycat.persist.model.SecondaryKey;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.vector.DependenceVector;
import fr.inria.jessy.vector.Vector;

/**
 * @author Masoud Saeida Ardekani
 * 
 */

@Entity
public class TestEntityClass extends JessyEntity{
	
//	public TestEntityClass(){
//		super();
//	}

	public TestEntityClass(Integer entityID, String data){
		super(new DependenceVector<String>(TestEntityClass.class.toString() + entityID));
		this.entityID=entityID;
		this.data=data;
	}

	@SecondaryKey(relate = MANY_TO_ONE)	
	private Integer entityID;

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
	public Integer getEntityID() {
		return entityID;
	}


	/**
	 * @param entityID the entityID to set
	 */
	public void setEntityID(Integer entityID) {
		this.entityID = entityID;
	}


	@Override
	public <T> String getLocalVectorSelfKey(T entityID) {
		return TestEntityClass.class.toString() + entityID;
	}


}
