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
public class Order_line extends JessyEntity {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Order_line(String entityID) {
		super(entityID);
	}

	public Order_line() {
		super("");
	}

	private int OL_O_ID;
	private String OL_D_ID;
	private String OL_W_ID;
	private String OL_NUMBER;
	private String OL_I_ID;
	private String OL_SUPPLY_W_ID;
	private Date OL_DELIVERY_D;
	private int OL_QUANTITY;
	private double OL_AMOUNT;
	private String OL_DIST_INFO;

	/**
	 * @return the data
	 */
	public int getOL_O_ID() {
		return OL_O_ID;
	}

	public String getOL_D_ID() {
		return OL_D_ID;
	}

	public String getOL_W_ID() {
		return OL_W_ID;
	}

	public String getOL_NUMBER() {
		return OL_NUMBER;
	}

	public String getOL_I_ID() {
		return OL_I_ID;
	}

	public String getOL_SUPPLY_W_ID() {
		return OL_SUPPLY_W_ID;
	}

	public Date getOL_DELIVERY_D() {
		return OL_DELIVERY_D;
	}

	public int getOL_QUANTITY() {
		return OL_QUANTITY;
	}

	public double getOL_AMOUNT() {
		return OL_AMOUNT;
	}

	public String getOL_DIST_INFO() {
		return OL_DIST_INFO;
	}

	/**
	 * @param data
	 *            the data to set
	 */
	public void setOL_O_ID(int OL_O_ID) {
		this.OL_O_ID = OL_O_ID;
	}

	public void setOL_D_ID(String OL_D_ID) {
		this.OL_D_ID = OL_D_ID;
	}

	public void setOL_W_ID(String OL_W_ID) {
		this.OL_W_ID = OL_W_ID;
	}

	public void setOL_NUMBER(String OL_NUMBER) {
		this.OL_NUMBER = OL_NUMBER;
	}

	public void setOL_I_ID(String OL_I_ID) {
		this.OL_I_ID = OL_I_ID;
	}

	public void setOL_SUPPLY_W_ID(String OL_SUPPLY_W_ID) {
		this.OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
	}

	public void setOL_DELIVERY_D(Date OL_DELIVERY_D) {
		this.OL_DELIVERY_D = OL_DELIVERY_D;
	}

	public void setOL_QUANTITY(int OL_QUANTITY) {
		this.OL_QUANTITY = OL_QUANTITY;
	}

	public void setOL_AMOUNT(double d) {
		this.OL_AMOUNT = d;
	}

	public void setOL_DIST_INFO(String OL_DIST_INFO) {
		this.OL_DIST_INFO = OL_DIST_INFO;
	}


	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(OL_O_ID);
		out.writeObject(OL_D_ID);
		out.writeObject(OL_W_ID);
		out.writeObject(OL_NUMBER);
		out.writeObject(OL_I_ID);
		out.writeObject(OL_SUPPLY_W_ID);
		out.writeObject(OL_DELIVERY_D);
		out.writeObject(OL_QUANTITY);
		out.writeObject(OL_AMOUNT);
		out.writeObject(OL_DIST_INFO);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		super.readExternal(in);
		OL_O_ID = (Integer) in.readObject();
		OL_D_ID = (String) in.readObject();
		OL_W_ID = (String) in.readObject();
		OL_NUMBER = (String) in.readObject();
		OL_I_ID = (String) in.readObject();
		OL_SUPPLY_W_ID = (String) in.readObject();
		OL_DELIVERY_D= (Date) in.readObject();
		OL_QUANTITY = (Integer) in.readObject();
		OL_AMOUNT = (Double) in.readObject();
		OL_DIST_INFO = (String) in.readObject();
	}
}
