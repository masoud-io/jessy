package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.*;

import java.util.*;

public class InsertData extends Transaction {
	
	public InsertData(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
			int i, j, k;
			Random rand = new Random();			
			String key;
			Warehouse wh = new Warehouse();
			
			District dis = new District();
			
			Customer cus = new Customer();
			
			Item it = new Item();
			
			/*for i  warehouses*/
			for(i=0; i<1; i++){			

				wh.setW_ID(Integer.toString(i));
				wh.setW_NAME("Warehouse"+i);
				wh.setW_TAX((int) rand.nextFloat());
				wh.setW_YTD((int) rand.nextFloat());
				key = "W_"+i;  /*W_id*/
				wh.setSecondaryKey(key); /*key = W_id*/
				write(wh);
				
				/*each warehouse has 10 district*/
				for(j=0; i<10; i++){
					
					dis.setD_ID(Integer.toString(j));
					dis.setD_W_ID(wh.getW_ID());
					dis.setD_NAME("District"+j);
					dis.setD_TAX((int) rand.nextFloat());
					dis.setD_YTD((int) rand.nextFloat());
					dis.setD_NEXT_O(1);
					
					key = "D_W_"+i+"_D_"+j;
					
					dis.setSecondaryKey(key);

					write(dis);
					/*each district has 3k customer*/
					for(k=0; k<3000; k++){
						cus.setC_ID(Integer.toString(k));
						cus.setC_D_ID(dis.getD_ID());
						cus.setC_W_ID(wh.getW_ID());
						
						key = "C_W_"+i+"_C_D_"+j+"_C_"+k;
						
						cus.setSecondaryKey(key);
						write(cus);						
						
					}
				}
					
			}
			
			/*for whole system, we have 10k different items*/
			/*10k items*/
			for(i=0; i<10000; i++){
				it.setI_ID(Integer.toString(i));
				it.setI_IM_ID(Integer.toString(i));
				it.setI_NAME("item"+i);
				it.setI_PRICE(rand.nextInt(5000 - 1) + 1);
				
				key = "I_"+i;
				
				it.setSecondaryKey(key);
				write(it);
			}
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
