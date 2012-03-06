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
public class Item extends JessyEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Item(String entityID) {
		super(Item.class.toString(), entityID);
	}

	public Item() {
		super("","");
	}

	private String I_ID;
	private String I_IM_ID;
	private String I_NAME;
	private int I_PRICE;
	private String I_DATA;
	
	/**
	 * @return the data
	 */
	public String getI_ID() {
		return I_ID;
	}

	public String getI_IM_ID() {
		return I_IM_ID;
	}
	
	public String getI_NAME() {
		return I_NAME;
	}
	
	public int getI_PRICE() {
		return I_PRICE;
	}
	
	public String getI_DATA() {
		return I_DATA;
	}
	
	/**
	 * @param data
	 *            the data to set
	 */
	public void setI_ID(String I_ID) {
		this.I_ID = I_ID;
	}
	
	public void setI_IM_ID(String I_IM_ID) {
		this.I_IM_ID = I_IM_ID;
	}
	
	public void setI_NAME(String I_NAME) {
		this.I_NAME = I_NAME;
	}
	
	public void setI_PRICE(int I_PRICE) {
		this.I_PRICE = I_PRICE;
	}
	
	public void setI_DATA(String I_DATA) {
		this.I_DATA = I_DATA;
	}

	@Override
	public String getKey() {
		return Customer.class.toString() + this.getSecondaryKey();
	}

}
