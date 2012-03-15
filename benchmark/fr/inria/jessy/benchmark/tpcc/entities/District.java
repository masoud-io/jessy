package fr.inria.jessy.benchmark.tpcc.entities;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import static com.sleepycat.persist.model.Relationship.*;

import com.sleepycat.persist.model.SecondaryKey;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.vector.DependenceVector;
import fr.inria.jessy.vector.Vector;

/**
 * @author Wang Haiyun & ZHAO Guang
 * 
 */


@Entity
public class District extends JessyEntity {

 

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public District(String entityID) {
		super(District.class.toString(), entityID);
	}
	
	public District() {
		super("","");
	}


	private String D_ID;
	private String D_W_ID;
	private String D_NAME;
	private String D_STREET_1;
	private String D_STREET_2;
	private String D_CITY;
	private String D_STATE;
	private String D_ZIP;
	private double D_TAX;
	private double D_YTD;
	private int D_NEXT_O;
	/**
	 * @return the data
	 */

	public String getD_ID() {
		return D_ID;
	}
	
	public String getD_W_ID() {
		return D_W_ID;
	}
	
	public String getD_NAME() {
		return D_NAME;
	}
	
	public String getD_STREET_1() {
		return D_STREET_1;
	}
	
	public String getD_STREET_2() {
		return D_STREET_2;
	}
	
	public String getD_CITY() {
		return D_CITY;
	}
	
	public String getD_STATE() {
		return D_STATE;
	}
	
	public String getD_ZIP() {
		return D_ZIP;
	}
	
	public double getD_TAX() {
		return D_TAX;
	}
	
	public double getD_YTD() {
		return D_YTD;
	}
	
	public int getD_NEXT_O() {
		return D_NEXT_O;
	}
	/**
	 * @param data
	 *            the data to set
	 */
	
	public void setD_ID(String D_ID) {
		this.D_ID = D_ID;
	}
	
	public void setD_W_ID(String D_W_ID) {
		this.D_W_ID = D_W_ID;
	}
	
	public void setD_NAME(String D_NAME) {
		this.D_NAME = D_NAME;
	}
	
	public void setD_STREET_1(String D_STREET_1) {
		this.D_STREET_1 = D_STREET_1;
	}
	
	public void setD_STREET_2(String D_STREET_2) {
		this.D_STREET_2 = D_STREET_2;
	}
	
	public void setD_CITY(String D_CITY) {
		this.D_CITY = D_CITY;
	}
	
	public void setD_STATE(String D_STATE) {
		this.D_STATE = D_STATE;
	}
	
	public void setD_ZIP(String D_ZIP) {
		this.D_ZIP = D_ZIP;
	}
	
	public void setD_TAX(double d) {
		this.D_TAX = d;
	}
	
	public void setD_YTD(double d) {
		this.D_YTD = d;
	}
	
	public void setD_NEXT_O(int D_NEXT_O) {
		this.D_NEXT_O = D_NEXT_O;
	}

	@Override
	public String getKey() {
		return Customer.class.toString() + this.getSecondaryKey();
	}
}
