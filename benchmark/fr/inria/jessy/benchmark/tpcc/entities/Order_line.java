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
public class Order_line extends JessyEntity {


	public Order_line(String entityID) {
		super(Order.class.toString(), entityID);
	}

	private String OL_O_ID;
	private String OL_D_ID;
	private String OL_W_ID;
	private String OL_NUMBER;
	private String OL_I_ID;
	private String OL_SUPPLY_W_ID;
	private date OL_DELIVERY_D;
	private int OL_QUANTITY;
	private int OL_AMOUNT;
	private String OL_DIST_INFO;

	/**
	 * @return the data
	 */
	public String getOL_O_ID() {
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
	
	public int getOL_AMOUNT() {
		return OL_AMOUNT;
	}
	
	public String getOL_DIST_INFO() {
		return OL_DIST_INFO;
	}
	
	/**
	 * @param data
	 *            the data to set
	 */
	public void setOL_O_ID(String OL_O_ID) {
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
	
	public void setOL_AMOUNT(int OL_AMOUNT) {
		this.OL_AMOUNT = OL_AMOUNT;
	}
	
	public void setOL_DIST_INFO(String OL_DIST_INFO) {
		this.OL_DIST_INFO = OL_DIST_INFO;
	}

	@Override
	public String getKey() {
		return Customer.class.toString() + this.getSecondaryKey();
	}

}
