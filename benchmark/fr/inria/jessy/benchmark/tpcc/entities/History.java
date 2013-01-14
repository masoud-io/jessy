package fr.inria.jessy.benchmark.tpcc.entities;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;

import com.sleepycat.persist.model.Entity;

import fr.inria.jessy.store.JessyEntity;

/**
 * @author Wang Haiyun & ZHAO Guang
 * 
 */


@Entity
public class History extends JessyEntity {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public History(String entityID) {
		super(entityID);
	}

	public History() {
		super("");
	}


	private String H_C_ID;
	private String H_C_D_ID;
	private String H_C_W_ID;
	private String H_D_ID;
	private String H_W_ID;
	private Date H_DATE;
	private double H_AMOUNT;
	private String H_DATA;
	/**
	 * @return the data
	 */

	public String getH_C_ID() {
		return H_C_ID;
	}

	public String getH_C_D_ID() {
		return H_C_D_ID;
	}
	public String getH_C_W_ID() {
		return H_C_W_ID;
	}
	public String getH_D_ID() {
		return H_D_ID;
	}
	public String getH_W_ID() {
		return H_W_ID;
	}

	public Date getH_DATE() {
		return H_DATE;
	}

	public double getH_AMOUNT() {
		return H_AMOUNT;
	}

	public String getH_DATA() {
		return H_DATA;
	}

	/**
	 * @param data
	 *            the data to set
	 */
	public void setH_C_ID(String H_C_ID) {
		this.H_C_ID = H_C_ID;
	}

	public void setH_C_D_ID(String H_C_D_ID) {
		this.H_C_D_ID = H_C_D_ID;
	}

	public void setH_C_W_ID(String H_C_W_ID) {
		this.H_C_W_ID = H_C_W_ID;
	}

	public void setH_D_ID(String H_D_ID) {
		this.H_D_ID = H_D_ID;
	}

	public void setH_W_ID(String H_W_ID) {
		this.H_W_ID = H_W_ID;
	}

	public void setH_DATE(Date H_DATE) {
		this.H_DATE = H_DATE;
	}

	public void setH_AMOUNT(double H_AMOUNT) {
		this.H_AMOUNT = H_AMOUNT;
	}

	public void setH_DATA(String H_DATA) {
		this.H_DATA = H_DATA;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(H_C_ID);
		out.writeObject(H_C_D_ID);
		out.writeObject(H_C_W_ID);
		out.writeObject(H_D_ID);
		out.writeObject(H_W_ID);
		out.writeObject(H_DATE);
		out.writeObject(H_AMOUNT);
		out.writeObject(H_DATA);

	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		super.readExternal(in);
		H_C_ID = (String) in.readObject();
		H_C_D_ID = (String) in.readObject();
		H_C_W_ID = (String) in.readObject();
		H_D_ID = (String) in.readObject();
		H_W_ID = (String) in.readObject();
		H_DATE = (Date) in.readObject();
		H_AMOUNT = (Double) in.readObject();
		H_DATA = (String) in.readObject();
	}

	@Override
	public void clearValue() {
		// TODO Auto-generated method stub
		
	}

}


