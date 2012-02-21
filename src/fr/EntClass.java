package fr;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import static com.sleepycat.persist.model.Relationship.*;

import com.sleepycat.persist.model.SecondaryKey;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.TestEntityClass;
import fr.inria.jessy.vector.DependenceVector;

/**
 * @author Masoud Saeida Ardekani
 * 
 */

@Entity
public class EntClass extends JessyEntity{
	
	public EntClass(){
		super(new DependenceVector<String>(TestEntityClass.class.toString()));
	}


	@SecondaryKey(relate = MANY_TO_ONE)	
	private String classValue;

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
	 * @return the classValue
	 */
	public String getClassValue() {
		return classValue;
	}


	/**
	 * @param classValue the classValue to set
	 */
	public void setClassValue(String classValue) {
		this.classValue = classValue;
	}


	@Override
	public <T> String getLocalVectorSelfKey(T entityID) {
		// TODO Auto-generated method stub
		return null;
	}


 

	

}
