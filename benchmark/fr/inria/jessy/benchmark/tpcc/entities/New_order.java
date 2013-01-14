package fr.inria.jessy.benchmark.tpcc.entities;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.sleepycat.persist.model.Entity;

import fr.inria.jessy.store.JessyEntity;

/**
 * @author Wang Haiyun & ZHAO Guang
 * 
 */

@Entity
public class New_order extends JessyEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public New_order(String entityID) {
		super(entityID);
	}

	public New_order() {
		super("");
	}

	private int NO_O_ID;
	private String NO_D_ID;
	private String NO_W_ID;

	/**
	 * @return the data
	 */


	public int getNO_O_ID() {
		return NO_O_ID;
	}

	public String getNO_D_ID() {
		return NO_D_ID;
	}

	public String getNO_W_ID() {
		return NO_W_ID;
	}
	/**
	 * @param data
	 *            the data to set
	 */
	public void setNO_O_ID(int NO_O_ID) {
		this.NO_O_ID = NO_O_ID;
	}

	public void setNO_D_ID(String NO_D_ID) {
		this.NO_D_ID = NO_D_ID;
	}

	public void setNO_W_ID(String NO_W_ID) {
		this.NO_W_ID = NO_W_ID;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(NO_O_ID);
		out.writeObject(NO_D_ID);
		out.writeObject(NO_W_ID);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		super.readExternal(in);
		NO_O_ID = (Integer) in.readObject();
		NO_D_ID = (String) in.readObject();
		NO_W_ID = (String) in.readObject();
	}

	@Override
	public void clearValue() {
		// TODO Auto-generated method stub
		
	}
}


