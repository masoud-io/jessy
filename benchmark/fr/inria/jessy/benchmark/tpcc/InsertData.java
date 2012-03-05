package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.*;
import fr.inria.jessy.transaction.*;
import java.util.*;

public class InsertData extends Transaction {
	
	public InsertData(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public boolean execute() {
		try {
			int i, j, k;
			Random rand = new Random();			
			String s;
			Warehouse wh = new Warehouse();
			
			District dis = new District();
			
			Customer cus = new Customer();
			
			Item it = new Item();
			
			/*for i  warehouses*/
			for(i=0; i<1; i++){			

				wh_d.setW_ID(Integer.toString(i));
				wh_d.setW_NAME("Warehouse"+i);
				wh_d.setW_TAX(rand.nextFloat());
				wh_d.setW_YTD(rand.nextFloat());
				s = "W_"+i;  /*W_id*/
				wh.setSecondaryKey(s); /*key = W_id*/
				write(wh);
				
				/*each warehouse has 10 district*/
				for(j=0; i<10; i++){
					
					dis_d.setD_ID(Integer.toString(j));
					dis_d.setW_ID(wh_d.getW_ID());
					dis_d.setD_NAME("D(istrict"+j);
					dis_d.setD_TAX(rand.nextFloat());
					dis_d.setD_YTD(rand.nextFloat());
					dis_d.setD_NEXT_O(1);
					
					s = "D_W_"+i+"_D_"+j;
					
					dis.setSecondaryKey(s);

					write(dis);
					
					for(k=0; k<3000; k++){
						cus_d.setC_ID(Integer.toString(k));
						cus_d.setC_D_ID(dis_d.getD_ID());
						cus_d.setC_W_ID(wh_d.getW_ID());
						
						s = "C_W_"+i+"_C_D_"+j+"_C_"+k;
						
						cus.setSecondaryKey(s);
						write(cus);
						
						
					}
				}
				
				

					

			}
			
			/*10k items*/
			for(i=0; i<10000; i++){
				it_d.I_ID = Integer.toString(i);
				it_d.I_IM_ID = Integer.toString(k);
				it_d.I_NAME = "item"+i;
				it_d.I_PRICE = rand.nextInt(5000 - 1) + 1;
				
				s = "I_"+i;
				
				it.setSecondaryKey(s);
				it.setData(it_d);
				write(it);
			}
				
			
			SampleEntityClass se=new SampleEntityClass("1", "sampleentity1");
			write(se);
			
			Sample2EntityClass se2=new Sample2EntityClass("1", "sampleentity2");	
			write(se2);
			
			SampleEntityClass readentity=read(SampleEntityClass.class, "1");			
			if (readentity.getData()=="sampleentity1"){
				write(new Sample2EntityClass("2", "sampleentity2-2"));
			}
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}		
	}

}
