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
public class New_order extends JessyEntity {

	public New_order() {
		super("", "");
	}

	public New_order(String entityID) {
		super(New_order.class.toString(), entityID);
		this.setSecondaryKey(entityID);
	}

	private String NO_O_ID;
	private String NO_D_ID;
	private String NO_W_ID;

	/**
	 * @return the data
	 */


	public String getNO_O_ID() {
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
	public void setNO_O_ID(String NO_O_ID) {
		this.NO_O_ID = NO_O_ID;
	}

	public void setNO_D_ID(String NO_D_ID) {
		this.NO_D_ID = NO_D_ID;
	}

	public void setNO_W_ID(String NO_W_ID) {
		this.NO_W_ID = NO_W_ID;
	}

	@Override
	public <T> String getLocalVectorSelfKey(T entityID) {
		return New_order.class.toString() + entityID;
	}

}


