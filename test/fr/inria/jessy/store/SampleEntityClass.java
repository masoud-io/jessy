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
public class SampleEntityClass extends JessyEntity{
	
	public SampleEntityClass(){
		super(new DependenceVector<String>(""));
	}

	public SampleEntityClass(String entityID, String data){
		super(new DependenceVector<String>(SampleEntityClass.class.toString() + entityID));
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
		return SampleEntityClass.class.toString() + entityID;
	}


}
