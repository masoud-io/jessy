package fr.inria.jessy.store;

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
public class Customer extends JessyEntity {

	public Customer() {
		super("", "");
	}

	public Customer(String entityID) {
		super(Customer.class.toString(), entityID);
		this.setSecondaryKey(entityID);
	}


	private String C_ID;
	private String C_D_ID;
	private String C_W_ID;
	private String C_FIRST;
	private String C_MIDDLE;
	private String C_LAST;
	private String C_STREET_1;
	private String C_STREET_2;
	private String C_CITY;
	private String C_STATE;
	private String C_ZIP;
	private String C_PHONE;
	private date C_SINCE;
	private String C_Credit;
	private int C_CREDIT_LIM;
	private int C_DISCOUNT;
	private int C_BALANCE;
	private int C_YTD_PAYMENT;
	private int C_DELIVERY_CNT;
	private String C_DATA;
	/**
	 * @return the data
	 */

	public String getC_ID() {
		return C_ID;
	}
	
	public String getC_D_ID() {
		return C_D_ID;
	}
	
	public String getC_W_ID() {
		return C_W_ID;
	}
	
	public String getC_FIRST() {
		return C_FIRST;
	}
	
	public String getC_MIDDLE() {
		return C_MIDDLE;
	}
	
	public String getC_LAST() {
		return C_LAST;
	}
	
	public String getC_STREET_1() {
		return C_STREET_1;
	}
	
	public String getC_STREET_2() {
		return C_STREET_2;
	}
	
	public String getC_CITY() {
		return C_CITY;
	}
	
	public String getC_STATE() {
		return C_STATE;
	}
	
	public String getC_ZIP() {
		return C_ZIP;
	}
	
	public String getC_PHONE() {
		return C_PHONE;
	}

	public date getC_SINCE() {
		return C_SINCE;
	}
	
	public String getC_Credit() {
		return C_Credit;
	}
	
	public int getC_CREDIT_LIM() {
		return C_CREDIT_LIM;
	}
	
	public int getC_DISCOUNT() {
		return C_DISCOUNT;
	}
	
	public int getC_BALANCE() {
		return C_BALANCE;
	}
	
	public int getC_YTD_PAYMENT() {
		return C_YTD_PAYMENT;
	}
	
	public int getC_DELIVERY_CNT() {
		return C_DELIVERY_CNT;
	}
	
	public String getC_DATA() {
		return C_DATA;
	}
	
	/**
	 * @param data
	 *            the data to set
	 */
	public void setData(Customer_data data) {
		this.C_ID = C_ID;
	}
	
	public void setC_ID(String C_ID) {
		this.C_ID = C_ID;
	}
	
	public void setC_D_ID(String C_D_ID) {
		this.C_D_ID = C_D_ID;
	}
	
	public void setC_W_ID(String C_W_ID) {
		this.C_W_ID = C_W_ID;
	}
	
	public void setData(String C_FIRST) {
		this.C_FIRST = C_FIRST;
	}
	
	public void setC_MIDDLE(String C_MIDDLE) {
		this.C_MIDDLE = C_MIDDLE;
	}
	
	public void setC_LAST(String C_LAST) {
		this.C_LAST = C_LAST;
	}
	
	public void setC_STREET_1(String C_STREET_1) {
		this.C_STREET_1 = C_STREET_1;
	}
	
	public void setData(String C_STREET_2) {
		this.C_STREET_2 = C_STREET_2;
	}
	
	public void setC_CITY(String C_CITY) {
		this.C_CITY = C_CITY;
	}
	
	public void setC_STATE(String C_STATE) {
		this.C_STATE = C_STATE;
	}
	
	public void setC_ZIP(String C_ZIP) {
		this.C_ZIP = C_ZIP;
	}
	
	public void setC_PHONE(String C_PHONE) {
		this.C_PHONE = C_PHONE;
	}
	
	public void setC_SINCE(date C_SINCE) {
		this.C_SINCE = C_SINCE;
	}

	public void setC_Credit(String C_Credit) {
		this.C_Credit = C_Credit;
	}
	
	public void setC_CREDIT_LIM(int C_CREDIT_LIM) {
		this.C_CREDIT_LIM = C_CREDIT_LIM;
	}
	
	public void setC_DISCOUNT(int C_DISCOUNT) {
		this.C_DISCOUNT = C_DISCOUNT;
	}
	
	public void setC_BALANCE(int C_BALANCE) {
		this.C_BALANCE = C_BALANCE;
	}
	
	public void setC_YTD_PAYMENT(int C_YTD_PAYMENT) {
		this.C_YTD_PAYMENT = C_YTD_PAYMENT;
	}
	
	public void setC_DELIVERY_CNT(int C_DELIVERY_CNT) {
		this.C_DELIVERY_CNT = C_DELIVERY_CNT;
	}
	
	public void setC_DATA(String C_DATA) {
		this.C_DATA = C_DATA;
	}
	@Override
	public <T> String getLocalVectorSelfKey(T entityID) {
		return Customer.class.toString() + entityID;
	}

}

