package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

import java.util.*;

public class OrderStatus extends Transaction {
	
	private String W_ID;
	private String D_ID;
	private String C_W_ID; 
	private String C_D_ID; 
	private String C_ID;
	private String C_LAST;
	private String O_ID;

	public OrderStatus(Jessy jessy) throws Exception {
		super(jessy);
		// TODO Auto-generated constructor stub
	}

	@Override
	public ExecutionHistory execute() {
		
    	try {
    		District district;
        	Customer customer;
        	Order order;
        	Order_line ol;
        	NURand nur;
    		int i,j;
        	
        	Random rand = new Random(System.currentTimeMillis());
        	int y = rand.nextInt(100-1)+1;  /* determine by C_LAST or by C_ID */
        	W_ID = "1";   /* warehouse number (W_ID) is constant  */
			
			D_ID = Integer.toString(rand.nextInt(10 - 1) + 1);  /* The district number (D_ID) is randomly selected within [1 ..10] */
			
			this.C_D_ID = D_ID;
			this.C_W_ID = W_ID;
			
			/* selection Customer */
			if(y>60) {   /* by C_ID */
				nur = new NURand(1023,1,3000);
        		C_ID = Integer.toString(nur.calculate());
        		customer = read(Customer.class, "C_W_"+C_W_ID + "_" + "C_D_"+C_D_ID + "_" + "C_"+C_ID);
			}
			else {       /* by C_LAST */
				nur = new NURand(255,0,999);
        		C_LAST = Integer.toString(nur.calculate());
       		
       /* !!! problem to retrieve, because we have no C_ID, just a subset of PK. so there is no PK helping do a read operation */
        		customer = read(Customer.class, "C_W_"+C_W_ID + "_" + "C_D_"+C_D_ID);
			}
		
			/* should we make a READ operation on District? but we need D_Next_O_ID to determine the number of orders. 
			 * So there will be a involved selection operation on the District table not mentioned in the benchmark */
			int tmp = 0;
			district = read(District.class, "D_W_"+ W_ID + "_" + "D_"+ D_ID);
			for(j=1;j<district.getD_NEXT_O();j++) {
				order = read(Order.class, "O_W_"+ C_W_ID + "_" + "O_D_"+ C_D_ID + "_" + "O_"+ j);
				if(order.getO_C_ID().equals(C_ID)) 
					tmp = j;
			}
			/* Selection of the most recent order */
			order = read(Order.class, "O_W_"+ C_W_ID + "_" + "O_D_"+ C_D_ID + "_" + "O_"+ tmp);
			
			/* Selection Order_line */
			for(i=1;i<=order.getO_OL_CNT();i++) {
				ol = read(Order_line.class, "OL_W_"+C_W_ID + "_" + "OL_D_"+C_D_ID + "_" + "OL_O_"+O_ID + "_" + "OL_"+i);
			}
			
			return commitTransaction();	
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
		return null;
	}

}
