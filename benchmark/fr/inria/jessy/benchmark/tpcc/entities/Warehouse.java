package fr.inria.jessy.benchmark.tpcc.entities;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.sleepycat.persist.model.Entity;

import fr.inria.jessy.store.JessyEntity;

@Entity
public class Warehouse extends JessyEntity {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Warehouse(String entityID) {
		super(entityID);
	}

	public Warehouse() {
		super("");
	}

	private String W_ID;
	private String W_NAME;
	private String W_STREET_1;
	private String W_STREET_2;
	private String W_CITY;
	private String W_STATE;
	private String W_ZIP;
	private double W_TAX;
	private double W_YTD;

	/**
	 * @return the data
	 */

	public String getW_ID() {
		return W_ID;
	}

	public String getW_NAME() {
		return W_NAME;
	}

	public String getW_STREET_1() {
		return W_STREET_1;
	}

	public String getW_STREET_2() {
		return W_STREET_2;
	}

	public String getW_CITY() {
		return W_CITY;
	}

	public String getW_STATE() {
		return W_STATE;
	}

	public String getW_ZIP() {
		return W_ZIP;
	}

	public double getW_TAX() {
		return W_TAX;
	}

	public double getW_YTD() {
		return W_YTD;
	}

	/**
	 * @param data
	 *            the data to set
	 */

	public void setW_ID(String W_ID) {
		this.W_ID = W_ID;
	}

	public void setW_NAME(String W_NAME) {
		this.W_NAME = W_NAME;
	}

	public void setW_STREET_1(String W_STREET_1) {
		this.W_STREET_1 = W_STREET_1;
	}

	public void setW_STREET_2(String W_STREET_2) {
		this.W_STREET_2 = W_STREET_2;
	}

	public void setW_CITY(String W_CITY) {
		this.W_CITY = W_CITY;
	}

	public void setW_STATE(String W_STATE) {
		this.W_STATE = W_STATE;
	}

	public void setW_ZIP(String W_ZIP) {
		this.W_ZIP = W_ZIP;
	}

	public void setW_TAX(double d) {
		this.W_TAX = d;
	}

	public void setW_YTD(double d) {
		this.W_YTD = d;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(W_ID);
		out.writeObject(W_NAME);
		out.writeObject(W_STREET_1);
		out.writeObject(W_STREET_2);
		out.writeObject(W_CITY);
		out.writeObject(W_STATE);
		out.writeObject(W_ZIP);
		out.writeObject(W_TAX);
		out.writeObject(W_YTD);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		super.readExternal(in);
		W_ID = (String) in.readObject();
		W_NAME = (String) in.readObject();
		W_STREET_1 = (String) in.readObject();
		W_STREET_2 = (String) in.readObject();
		W_CITY = (String) in.readObject();
		W_STATE = (String) in.readObject();
		W_ZIP = (String) in.readObject();
		W_TAX = (Double) in.readObject();
		W_YTD = (Double) in.readObject();
	}
}
