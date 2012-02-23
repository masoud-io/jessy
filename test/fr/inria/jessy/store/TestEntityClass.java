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
	
	public TestEntityClass(){
		super(new DependenceVector<String>(""));
	}

	public TestEntityClass(String entityID, String data){
		super(new DependenceVector<String>(TestEntityClass.class.toString() + entityID));
		this.setSecondaryKey(entityID);
		this.data=data;
	}

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

 
	@Override
	public <T> String getLocalVectorSelfKey(T entityID) {
		return TestEntityClass.class.toString() + entityID;
	}


}
