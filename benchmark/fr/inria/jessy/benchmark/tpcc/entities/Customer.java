package fr.inria.jessy.benchmark.tpcc.entities;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.SecondaryKey;

import fr.inria.jessy.store.JessyEntity;

/**
 * @author Wang Haiyun & ZHAO Guang
 * 
 */

@Entity
public class Customer extends JessyEntity implements Externalizable{


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Customer(String entityID) {
		super(entityID);
	}
	
	public Customer() {
		super("");
	}

	private String C_ID;
	
	@SecondaryKey(relate = MANY_TO_ONE)
	private String C_D_ID;
	
	@SecondaryKey(relate = MANY_TO_ONE)
	private String C_W_ID;
	
	private String C_FIRST;
	private String C_MIDDLE;
	
	@SecondaryKey(relate = MANY_TO_ONE)
	private String C_LAST;
	
	private String C_STREET_1;
	private String C_STREET_2;
	private String C_CITY;
	private String C_STATE;
	private String C_ZIP;
	private String C_PHONE;
	private Date C_SINCE;
	private String C_Credit;
	private double C_CREDIT_LIM;
	private double C_DISCOUNT;
	private double C_BALANCE;
	private double C_YTD_PAYMENT;
	private int C_PAYMENT_CNT;
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

	public Date getC_SINCE() {
		return C_SINCE;
	}

	public String getC_Credit() {
		return C_Credit;
	}

	public double getC_CREDIT_LIM() {
		return C_CREDIT_LIM;
	}

	public double getC_DISCOUNT() {
		return C_DISCOUNT;
	}

	public double getC_BALANCE() {
		return C_BALANCE;
	}

	public double getC_YTD_PAYMENT() {
		return C_YTD_PAYMENT;
	}
	
	public int getC_PAYMENT_CNT() {
		return C_PAYMENT_CNT;
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

	public void setC_ID(String C_ID) {
		this.C_ID = C_ID;
	}

	public void setC_D_ID(String C_D_ID) {
		this.C_D_ID = C_D_ID;
	}

	public void setC_W_ID(String C_W_ID) {
		this.C_W_ID = C_W_ID;
	}

	public void setC_FIRST(String C_FIRST) {
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

	public void setC_STREET_2(String C_STREET_2) {
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

	public void setC_SINCE(Date C_SINCE) {
		this.C_SINCE = C_SINCE;
	}

	public void setC_Credit(String C_Credit) {
		this.C_Credit = C_Credit;
	}

	public void setC_CREDIT_LIM(double d) {
		this.C_CREDIT_LIM = d;
	}

	public void setC_DISCOUNT(double d) {
		this.C_DISCOUNT = d;
	}

	public void setC_BALANCE(double d) {
		this.C_BALANCE = d;
	}

	public void setC_YTD_PAYMENT(double d) {
		this.C_YTD_PAYMENT = d;
	}

	public void setC_PAYMENT_CNT(int C_PAYMENT_CNT) {
		this.C_PAYMENT_CNT = C_PAYMENT_CNT;
	}
	
	public void setC_DELIVERY_CNT(int C_DELIVERY_CNT) {
		this.C_DELIVERY_CNT = C_DELIVERY_CNT;
	}

	public void setC_DATA(String C_DATA) {
		this.C_DATA = C_DATA;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);

		C_ID=(String)in.readObject();
		C_D_ID=(String)in.readObject();
		C_W_ID=(String)in.readObject();
		C_FIRST=(String)in.readObject();
		C_MIDDLE=(String)in.readObject();
		C_LAST=(String)in.readObject();
		C_STREET_1=(String)in.readObject();
		C_STREET_2=(String)in.readObject();
		C_CITY=(String)in.readObject();
		C_STATE=(String)in.readObject();
		C_ZIP=(String)in.readObject();
		C_PHONE=(String)in.readObject();
		C_SINCE=(Date)in.readObject();
		C_Credit=(String)in.readObject();
		C_CREDIT_LIM=in.readDouble();
		C_DISCOUNT=in.readDouble();
		C_BALANCE=in.readDouble();
		C_YTD_PAYMENT=in.readDouble();
		C_PAYMENT_CNT=in.readInt();
		C_DELIVERY_CNT=in.readInt();
		C_DATA=(String)in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);

		out.writeObject(C_ID);
		out.writeObject(C_D_ID);
		out.writeObject(C_W_ID);
		out.writeObject(C_FIRST);
		out.writeObject(C_MIDDLE);
		out.writeObject(C_LAST);
		out.writeObject(C_STREET_1);
		out.writeObject(C_STREET_2);
		out.writeObject(C_CITY);
		out.writeObject(C_STATE);
		out.writeObject(C_ZIP);
		out.writeObject(C_PHONE);
		out.writeObject(C_SINCE);
		out.writeObject(C_Credit);
		out.writeDouble(C_CREDIT_LIM);
		out.writeDouble(C_DISCOUNT);
		out.writeDouble(C_BALANCE);
		out.writeDouble(C_YTD_PAYMENT);
		out.writeInt(C_PAYMENT_CNT);
		out.writeInt(C_DELIVERY_CNT);
		out.writeObject(C_DATA);
	}


}
