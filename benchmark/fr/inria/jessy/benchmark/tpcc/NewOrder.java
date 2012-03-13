package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.*;

import java.util.*;

public class NewOrder extends Transaction {
	
	public NewOrder(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
			int i, j, k;
			Random rand = new Random(System.currentTimeMillis());			
			Warehouse wh = new Warehouse();			
			District dis = new District();			
			Customer cus = new Customer();			
			Item it = new Item();
			String district_id;
			String customer_id;
			NURand nu;
			
			wh = read(Warehouse.class, "W_1");
			/* The district number (D_ID) is randomly selected within [1 .. 10] from the home warehouse (D_W_ID =
			W_ID).*/
			district_id = Integer.toString(rand.nextInt(10 - 1) + 1);
			dis = read(District.class, "D_W_"+wh.getW_ID()+"D_"+district_id);
			
			/*
			The non-uniform random customer number (C_ID) is selected using the NURand (1023,1,3000) function from
			the selected district number (C_D_ID = D_ID) and the home warehouse number (C_W_ID = W_ID).
			*/
			nu = new NURand(1023,1,3000);
			customer_id = Integer.toString(nu.calculate());
			cus = read(Customer.class, "C_W_"+wh.getW_ID()+"_C_D_"+dis.getD_ID()+"_C_"+customer_id);
			
			
			
			

			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
