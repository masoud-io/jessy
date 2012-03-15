package fr.inria.jessy.benchmark.tpcc;


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
					
				/*TODO
				 * add the if fail, return abortTransaction();
				 */
				
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
