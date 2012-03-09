package fr.inria.jessy.benchmark.tpcc.entities;

import com.sleepycat.persist.model.Entity;
import fr.inria.jessy.store.JessyEntity;

@Entity
public class Warehouse extends JessyEntity {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Warehouse(String entityID) {
		super(Warehouse.class.toString(), entityID);
	}
	
	public Warehouse() {
		super("", "");
	}

	private String W_ID;
	private String W_NAME;
	private String W_STREET_1;
	private String W_STREET_2;
	private String W_CITY;
	private String W_STATE;
	private String W_ZIP;
	private int W_TAX;
	private int W_YTD;

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

	public int getW_TAX() {
		return W_TAX;
	}

	public int getW_YTD() {
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

	public void setW_TAX(int W_TAX) {
		this.W_TAX = W_TAX;
	}

	public void setW_YTD(int W_YTD) {
		this.W_YTD = W_YTD;
	}

	@Override
	public String getKey() {
		return Customer.class.toString() + this.getSecondaryKey();
	}
}
