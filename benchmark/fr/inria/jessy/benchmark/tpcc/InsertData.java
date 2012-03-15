package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.*;

import java.util.*;

public class InsertData extends Transaction {
	/*tpcc   section 4.3*/
	public InsertData(Jessy jessy) throws Exception{
		super(jessy);
	}

	@Override
	public ExecutionHistory execute() {
		try {
			int i, j, k, l;
			Random rand = new Random(System.currentTimeMillis());			
			String[] lastnames = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
			String lastname;
			Warehouse wh;			
			District dis;			
			Customer cus;			
			Item it;
			
			/*for i  warehouses*/
			for(i=0; i<1; i++){			
				wh = new Warehouse("W_"+i);
				wh.setW_ID(Integer.toString(i));
				wh.setW_NAME(NString.generate(6, 10));
				wh.setW_STREET_1(NString.generate(10, 20));
				wh.setW_STREET_2(NString.generate(10, 20));
				wh.setW_CITY(NString.generate(10, 20));
				wh.setW_STATE(NString.generateFix(2));
				wh.setW_ZIP(Integer.toString(rand.nextInt(9999 - 1) + 1)+"11111");
				wh.setW_TAX(rand.nextFloat()*0.200);
				wh.setW_YTD(300000.00);

				create(wh);
				/*each warehouse hase 100,000 rows in the STOCK table*/
				for(j=0; j<100000; j++){
					//TODO
				}
				
				
				/*each warehouse has 10 district*/
				for(j=0; i<10; i++){
					dis = new District("D_W_"+i+"_D_"+j);
					dis.setD_ID(Integer.toString(j));
					dis.setD_W_ID(wh.getW_ID());
					dis.setD_NAME(NString.generate(6, 10));
					dis.setD_STREET_1(NString.generate(10, 20));
					dis.setD_STREET_2(NString.generate(10, 20));
					dis.setD_CITY(NString.generate(10, 20));
					dis.setD_STATE(NString.generateFix(2));
					dis.setD_ZIP(Integer.toString(rand.nextInt(10000))+"11111"); /*[0....9999]+111111*/
					dis.setD_TAX(rand.nextFloat()*0.2000);
					dis.setD_YTD(300000.00);
					dis.setD_NEXT_O(3001);

					create(dis);
					
					/*each district has 3k customer*/
					for(k=0; k<3000; k++){
						cus=new Customer("C_W_"+i+"_C_D_"+j+"_C_"+k);
						cus.setC_ID(Integer.toString(k));
						cus.setC_D_ID(dis.getD_ID());
						cus.setC_W_ID(wh.getW_ID());
						lastname = "";
						for(l=0; l<3; l++){
							lastname = lastname+lastnames[rand.nextInt(10)]; /* 0..9 */
						}
						cus.setC_LAST(lastname);
						cus.setC_MIDDLE("OE");
						cus.setC_FIRST(NString.generate(8, 16));
						cus.setC_STREET_1(NString.generate(10, 20));
						cus.setC_STREET_2(NString.generate(10, 20));
						cus.setC_CITY(NString.generate(10, 20));
						cus.setC_STATE(NString.generateFix(2));
						cus.setC_ZIP(Integer.toString(rand.nextInt(10000))+"11111"); /*[0....9999]+111111*/
						cus.setC_PHONE(NString.generateFix(16));
						cus.setC_SINCE(new Date());
						if(rand.nextInt(10) == 0){
							/* 10% chance */
							cus.setC_Credit("BC");
						}
						else cus.setC_Credit("GC");
						cus.setC_CREDIT_LIM(50000.00);
						cus.setC_DISCOUNT(rand.nextFloat()*0.5000);
						cus.setC_BALANCE(-10.00);
						cus.setC_YTD_PAYMENT(10.00);
						cus.setC_PAYMENT_CNT(1);
						cus.setC_DELIVERY_CNT(0);
						cus.setC_DATA(NString.generate(300, 500));
						
						create(cus);
						
						
					}
				}
					
			}
			
			/*for whole system, we have 10k different items*/
			/*10k items*/
			for(i=0; i<10000; i++){
				it = new Item("I_"+i);
				it.setI_ID(Integer.toString(i));
				it.setI_IM_ID(Integer.toString(i));
				it.setI_NAME("item"+i);
				it.setI_PRICE(rand.nextInt(5000 - 1) + 1);
				
				create(it);
			}
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
