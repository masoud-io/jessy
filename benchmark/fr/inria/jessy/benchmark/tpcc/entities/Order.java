package fr.inria.jessy.benchmark.tpcc.entities;

import java.util.Date;

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
public class Order extends JessyEntity {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public Order(String entityID) {
		super(Order.class.toString(), entityID);
	}
	
	public Order() {
		super("","");
	}

	private int O_ID;
	private String O_D_ID;
	private String O_W_ID;
	private String O_C_ID;
	private Date O_ENTRY_D;
	private String O_CARRIER_ID;
	private int O_OL_CNT;
	private int O_ALL_LOCAL;
	/**
	 * @return the data
	 */
	public int getO_ID() {
		return O_ID;
	}
	
	public String getO_D_ID() {
		return O_D_ID;
	}
	
	public String getO_W_ID() {
		return O_W_ID;
	}
	
	public String getO_C_ID() {
		return O_C_ID;
	}
	
	public Date getO_ENTRY_D() {
		return O_ENTRY_D;
	}
	
	public String getO_CARRIER_ID() {
		return O_CARRIER_ID;
	}
	
	public int getO_OL_CNT() {
		return O_OL_CNT;
	}
	
	public int getO_ALL_LOCAL() {
		return O_ALL_LOCAL;
	}
	/**
	 * @param data
	 *            the data to set
	 */
	public void setO_ID(int O_ID) {
		this.O_ID = O_ID;
	}
	
	public void setO_D_ID(String O_D_ID) {
		this.O_D_ID = O_D_ID;
	}
	
	public void setO_W_ID(String O_W_ID) {
		this.O_W_ID = O_W_ID;
	}
	
	public void setO_C_ID(String O_C_ID) {
		this.O_C_ID = O_C_ID;
	}
	
	public void setO_ENTRY_D(Date O_ENTRY_D) {
		this.O_ENTRY_D = O_ENTRY_D;
	}
	
	public void setO_CARRIER_ID(String O_CARRIER_ID) {
		this.O_CARRIER_ID = O_CARRIER_ID;
	}
	
	public void setO_OL_CNT(int O_OL_CNT) {
		this.O_OL_CNT = O_OL_CNT;
	}
	
	public void setO_ALL_LOCAL(int O_ALL_LOCAL) {
		this.O_ALL_LOCAL = O_ALL_LOCAL;
	}

	@Override
	public String getKey() {
		return Customer.class.toString() + this.getSecondaryKey();
	}

}
