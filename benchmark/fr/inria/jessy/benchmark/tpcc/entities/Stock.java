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
public class Stock extends JessyEntity {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Stock(String entityID) {
		super(Stock.class.toString(), entityID);
	}

	private String S_I_ID;
	private String S_W_ID;
	private int S_QUANTITY;
	private String S_DIST_01;
	private String S_DIST_02;
	private String S_DIST_03;
	private String S_DIST_04;
	private String S_DIST_05;
	private String S_DIST_06;
	private String S_DIST_07;
	private String S_DIST_08;
	private String S_DIST_09;
	private String S_DIST_10;
	private int S_YTD;
	private int S_ORDER_CNT;
	private int S_REMOTE_CNT;
	private String S_DATA;
	
	/**
	 * @return the data
	 */
	public String getS_I_ID() {
		return S_I_ID;
	}
	
	public String getS_W_ID() {
		return S_W_ID;
	}
	
	public int getS_QUANTITY() {
		return S_QUANTITY;
	}
	
	public String getS_DIST_01() {
		return S_DIST_01;
	}
	
	public String getS_DIST_02() {
		return S_DIST_02;
	}
	
	public String getS_DIST_03() {
		return S_DIST_03;
	}
	
	public String getS_DIST_04() {
		return S_DIST_04;
	}
	
	public String getS_DIST_05() {
		return S_DIST_05;
	}
	
	public String getS_DIST_06() {
		return S_DIST_06;
	}
	
	public String getS_DIST_07() {
		return S_DIST_07;
	}
	
	public String getS_DIST_08() {
		return S_DIST_08;
	}
	
	public String getS_DIST_09() {
		return S_DIST_09;
	}
	
	public String getS_DIST_10() {
		return S_DIST_10;
	}
	
	public int getS_YTD() {
		return S_YTD;
	}
	
	public int getS_ORDER_CNT() {
		return S_ORDER_CNT;
	}
	
	public int getS_REMOTE_CNT() {
		return S_REMOTE_CNT;
	}
	
	public String getS_DATA() {
		return S_DATA;
	}

	/**
	 * @param data
	 *            the data to set
	 */
	public void setS_I_ID(String S_I_ID) {
		this.S_I_ID = S_I_ID;
	}
	
	public void setS_W_ID(String S_W_ID) {
		this.S_W_ID = S_W_ID;
	}
	
	public void setS_QUANTITY(int S_QUANTITY) {
		this.S_QUANTITY = S_QUANTITY;
	}

	public void setS_DIST_01(String S_DIST_01) {
		this.S_DIST_01 = S_DIST_01;
	}
	
	public void setS_DIST_02(String S_DIST_02) {
		this.S_DIST_02 = S_DIST_02;
	}
	
	public void setS_DIST_03(String S_DIST_03) {
		this.S_DIST_03 = S_DIST_03;
	}
	
	public void setS_DIST_04(String S_DIST_04) {
		this.S_DIST_04 = S_DIST_04;
	}
	
	public void setS_DIST_05(String S_DIST_05) {
		this.S_DIST_05 = S_DIST_05;
	}
	
	public void setS_DIST_06(String S_DIST_06) {
		this.S_DIST_06 = S_DIST_06;
	}
	
	public void setS_DIST_07(String S_DIST_07) {
		this.S_DIST_07 = S_DIST_07;
	}
	
	public void setS_DIST_08(String S_DIST_08) {
		this.S_DIST_08 = S_DIST_08;
	}
	
	public void setS_DIST_09(String S_DIST_09) {
		this.S_DIST_09 = S_DIST_09;
	}
	
	public void setS_DIST_10(String S_DIST_10) {
		this.S_DIST_10 = S_DIST_10;
	}

	public void setS_YTD(int S_YTD) {
		this.S_YTD = S_YTD;
	}
	
	public void setS_ORDER_CNT(int S_ORDER_CNT) {
		this.S_ORDER_CNT = S_ORDER_CNT;
	}
	
	public void setS_REMOTE_CNT(int S_REMOTE_CNT) {
		this.S_REMOTE_CNT = S_REMOTE_CNT;
	}
	
	public void setS_DATA(String S_DATA) {
		this.S_DATA = S_DATA;
	}

	@Override
	public String getKey() {
		return Customer.class.toString() + this.getSecondaryKey();
	}


}
