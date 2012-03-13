package fr.inria.jessy.benchmark.tpcc;

import static org.junit.Assert.*;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.*;

public class  TpccVerifyInsertedData extends Transaction {
	
	public TpccVerifyInsertedData(Jessy jessy) throws Exception{
		super(jessy);
	}
	
	private Warehouse w;
	private District d;
	private Customer c;
	private Item i;
	
	
	@Override
	public ExecutionHistory execute() {
		try {
				w = read(Warehouse.class, "W_1");
				d= read(District.class, "D_W_1_D_1" );
				c= read(Customer.class, "C_W_1_C_D_1_C_1");
				i= read(Item.class, "I_1");
					
				assertEquals("Warehouse id", "W_1", w.getW_ID());
				assertEquals("District id", "D_1", d.getD_ID());
				assertEquals("customer id", "C_1", c.getC_ID());
				assertEquals("item id", "I_1", i.getI_ID());
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}



	public Warehouse getW() {
		return w;
	}


	public void setW(Warehouse w) {
		this.w = w;
	}


	public District getD() {
		return d;
	}


	public void setD(District d) {
		this.d = d;
	}


	public Customer getC() {
		return c;
	}


	public void setC(Customer c) {
		this.c = c;
	}


	public Item getI() {
		return i;
	}


	public void setI(Item i) {
		this.i = i;
	}

}
