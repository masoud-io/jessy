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
public class Item extends JessyEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Item(String entityID) {
		super(entityID);
	}

	public Item() {
		super("");
	}


	private String I_ID;
	private String I_IM_ID;
	private String I_NAME;
	private double I_PRICE;
	private String I_DATA;

	/**
	 * @return the data
	 */
	public String getI_ID() {
		return I_ID;
	}

	public String getI_IM_ID() {
		return I_IM_ID;
	}

	public String getI_NAME() {
		return I_NAME;
	}

	public double getI_PRICE() {
		return I_PRICE;
	}

	public String getI_DATA() {
		return I_DATA;
	}

	/**
	 * @param data
	 *            the data to set
	 */
	public void setI_ID(String I_ID) {
		this.I_ID = I_ID;
	}

	public void setI_IM_ID(String I_IM_ID) {
		this.I_IM_ID = I_IM_ID;
	}

	public void setI_NAME(String I_NAME) {
		this.I_NAME = I_NAME;
	}

	public void setI_PRICE(double d) {
		this.I_PRICE = d;
	}

	public void setI_DATA(String I_DATA) {
		this.I_DATA = I_DATA;
	}


	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(I_ID);
		out.writeObject(I_IM_ID);
		out.writeObject(I_NAME);
		out.writeObject(I_PRICE);
		out.writeObject(I_DATA);

	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		super.readExternal(in);
		I_ID = (String) in.readObject();
		I_IM_ID = (String) in.readObject();
		I_NAME = (String) in.readObject();
		I_PRICE = (Double) in.readObject();
		I_DATA = (String) in.readObject();
	}

}
