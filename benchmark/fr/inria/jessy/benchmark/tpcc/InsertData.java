package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.*;

import java.util.*;

/**
 * @author Wang Haiyun & ZHAO Guang
 * 
 */
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
			int[] randCustomerId = new int[3000]; // random permutation, used while creating Order table
			for(i=0; i<3000; i++){
				randCustomerId[i] = i;
			}
			for(i=0; i<3000; i++){
				randCustomerId[i] = randCustomerId[rand.nextInt(3000)];
			}
			String lastname;
			Warehouse wh;	
			Stock st;
			District dis;			
			Customer cus;			
			Item it;
			History hi;
			Order or;
			Order_line ol;
			New_order no;
			NURand nu;
			
			/*for i  warehouses*/
			for(i=0; i<1; i++){			
				wh = new Warehouse("W_"+i);
				wh.setW_ID(Integer.toString(i));
				wh.setW_NAME(NString.generate(6, 10));
				wh.setW_STREET_1(NString.generate(10, 20));
				wh.setW_STREET_2(NString.generate(10, 20));
				wh.setW_CITY(NString.generate(10, 20));
				wh.setW_STATE(NString.generateFix(2));
				wh.setW_ZIP(Integer.toString(rand.nextInt(9999 + 1))+"11111");
				wh.setW_TAX(rand.nextFloat()*0.200);
				wh.setW_YTD(300000.00);

				create(wh);
				/*each warehouse has 100,000 rows in the STOCK table*/
				for(j=0; j<100000; j++){
					st = new Stock("S_W_"+wh.getW_ID()+"S_I_"+j);
					st.setS_I_ID(Integer.toString(j));
					st.setS_W_ID(wh.getW_ID());
					st.setS_QUANTITY(rand.nextInt(100-10+1)+10); //[10..100]
					st.setS_DIST_01(NString.generateFix(24));
					st.setS_DIST_02(NString.generateFix(24));
					st.setS_DIST_03(NString.generateFix(24));
					st.setS_DIST_04(NString.generateFix(24));
					st.setS_DIST_05(NString.generateFix(24));
					st.setS_DIST_06(NString.generateFix(24));
					st.setS_DIST_07(NString.generateFix(24));
					st.setS_DIST_08(NString.generateFix(24));
					st.setS_DIST_09(NString.generateFix(24));
					st.setS_DIST_10(NString.generateFix(24));
					st.setS_YTD(0);
					st.setS_ORDER_CNT(0);
					st.setS_REMOTE_CNT(0);
					if(rand.nextInt(10) == 0){ //10% chance  putting "ORIGINAL" in S_DATA
						st.setS_DATA(NString.original(26, 50));			
					}
					else st.setS_DATA(NString.generate(26, 50));
					create(st);
				}
				
				
				/*each warehouse has 10 district*/
				for(j=0; j<10; j++){
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
						if(k<1000){//first 1000 customers
							lastname = "";
							for(l=0; l<3; l++){
								lastname = lastname+lastnames[rand.nextInt(10)]; /* 0..9 */
							}
							cus.setC_LAST(lastname);
						}
						else{
							nu = new NURand(255, 0, 999);
							cus.setC_LAST(Integer.toString(nu.calculate()));
						}
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
						
						//each customer has 1 history
						hi = new History("H_C_W_"+wh.getW_ID()+"_H_C_D_"+dis.getD_ID()+"_H_C_"+cus.getC_ID());
						hi.setH_C_W_ID(wh.getW_ID());
						hi.setH_C_D_ID(dis.getD_ID());
						hi.setH_C_ID(cus.getC_ID());
						hi.setH_DATE(new Date());
						hi.setH_AMOUNT(10.00);
						hi.setH_DATA(NString.generate(12, 24));
						create(hi);
															
					}
					
					/*each district has 3000 order*/
					for(k=0; k< 3000; k++){
						or = new Order("O_W_"+wh.getW_ID()+"_O_D_"+dis.getD_ID()+"_O_"+k);
						or.setO_C_ID(Integer.toString(randCustomerId[k]));
						or.setO_W_ID(wh.getW_ID());
						or.setO_D_ID(dis.getD_ID());
						or.setO_ENTRY_D(new Date());
						if(k<2101)
							or.setO_CARRIER_ID(Integer.toString(rand.nextInt(10)+1));
						else or.setO_CARRIER_ID(null);
						or.setO_OL_CNT(rand.nextInt(15-5+1)+5); //[5 .. 15]
						or.setO_ALL_LOCAL(1);
						create(or);
						
						/*each order has o_ol_cnt order_lines*/
						for(l=0; l< or.getO_OL_CNT(); l++){
							ol = new Order_line("OL_W_"+wh.getW_ID()+"_OL_D_"+dis.getD_ID()+"_OL_O_"+or.getO_ID()+"_OL_"+l);
							ol.setOL_W_ID(wh.getW_ID());
							ol.setOL_D_ID(dis.getD_ID());
							ol.setOL_O_ID(or.getO_ID());
							ol.setOL_NUMBER(Integer.toString(l));
							ol.setOL_I_ID(Integer.toString(rand.nextInt(100000)+1)); //[1..100000]
							ol.setOL_SUPPLY_W_ID(wh.getW_ID());
							if(l < 2101){
								ol.setOL_DELIVERY_D(or.getO_ENTRY_D());
								ol.setOL_AMOUNT(0.00);
							}
							else{
								ol.setOL_DELIVERY_D(null);
								ol.setOL_AMOUNT(rand.nextFloat()*9999.99); //[0.01 .. 9999.99]
							}
							ol.setOL_QUANTITY(5);
							ol.setOL_DIST_INFO(NString.generateFix(24));
							create(ol);
						}
					}
					/*each district has 900 new_order*/
					for(k=0; k< 900; k++){
						no = new New_order("NO_W_"+wh.getW_ID()+"_NO_D_"+dis.getD_ID()+"_NO_O_"+2101+k);
						no.setNO_W_ID(wh.getW_ID());
						no.setNO_D_ID(dis.getD_ID());
						no.setNO_O_ID(2101+k);
						create(no);
					}
				}
					
			}
			
			/*for whole system, we have 100k different types of item*/
			for(i=0; i<100000; i++){
				it = new Item("I_"+i);
				it.setI_ID(Integer.toString(i));
				it.setI_IM_ID(Integer.toString(rand.nextInt(10000)+1)); //[1..10000]
				it.setI_NAME(NString.generate(14, 24));
				it.setI_PRICE(rand.nextFloat()*100.00);//[1.00 .. 100.00]
				if(rand.nextInt(10) == 0){ //10% chance  putting "ORIGINAL" in I_DATA
					it.setI_DATA(NString.original(26, 50));			
				}
				else it.setI_DATA(NString.generate(26, 50));
				create(it);
			}
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
