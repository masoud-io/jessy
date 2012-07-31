package fr.inria.jessy.entity;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.sleepycat.persist.model.Entity;

import fr.inria.jessy.store.JessyEntity;

/**
 * @author Masoud Saeida Ardekani
 * 
 */

@Entity
public class Sample2EntityClass extends JessyEntity {

	public Sample2EntityClass() {
		super("");
	}

	public Sample2EntityClass(String entityID, String data) {
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
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(data);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		data = (String) in.readObject();
	}
}
