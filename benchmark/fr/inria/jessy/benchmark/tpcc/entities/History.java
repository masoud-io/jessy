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
public class History extends JessyEntity {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	public History(String entityID) {
		super(History.class.toString(), entityID);
	}


	private String H_C_ID;
	private String H_C_D_ID;
	private String H_C_W_ID;
	private String H_D_ID;
	private String H_W_ID;
	private Date H_DATE;
	private int H_AMOUNT;
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

	public int getH_AMOUNT() {
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
	
	public void setH_AMOUNT(int H_AMOUNT) {
		this.H_AMOUNT = H_AMOUNT;
	}
	
	public void setH_DATA(String H_DATA) {
		this.H_DATA = H_DATA;
	}

	@Override
	public String getKey() {
		return Customer.class.toString() + this.getSecondaryKey();
	}

}


