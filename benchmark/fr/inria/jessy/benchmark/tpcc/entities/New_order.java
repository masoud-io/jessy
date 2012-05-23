package fr.inria.jessy.benchmark.tpcc.entities;

import com.sleepycat.persist.model.Entity;

import fr.inria.jessy.store.JessyEntity;

/**
 * @author Wang Haiyun & ZHAO Guang
 * 
 */

@Entity
public class New_order extends JessyEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public New_order(String entityID) {
		super(entityID);
	}
	
	public New_order() {
		super("");
	}

	private int NO_O_ID;
	private String NO_D_ID;
	private String NO_W_ID;

	/**
	 * @return the data
	 */


	public int getNO_O_ID() {
		return NO_O_ID;
	}
	
	public String getNO_D_ID() {
		return NO_D_ID;
	}
	
	public String getNO_W_ID() {
		return NO_W_ID;
	}
	/**
	 * @param data
	 *            the data to set
	 */
	public void setNO_O_ID(int NO_O_ID) {
		this.NO_O_ID = NO_O_ID;
	}

	public void setNO_D_ID(String NO_D_ID) {
		this.NO_D_ID = NO_D_ID;
	}

	public void setNO_W_ID(String NO_W_ID) {
		this.NO_W_ID = NO_W_ID;
	}

}


