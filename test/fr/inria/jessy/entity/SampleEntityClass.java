package fr.inria.jessy.entity;

import com.sleepycat.persist.model.Entity;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.JessyEntity;

/**
 * @author Masoud Saeida Ardekani
 * 
 */

@Entity
public class SampleEntityClass extends JessyEntity {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	public SampleEntityClass() {
		super("");
	}

	public SampleEntityClass(String entityID, String data) {
		super(entityID);
		this.data = data;
	}

	private String data;

	/**
	 * @return the data
	 */
	public String getData() {
		return data;
	}

	/**
	 * @param data
	 *            the data to set
	 */
	public void setData(String data) {
		this.data = data;
	}

}
