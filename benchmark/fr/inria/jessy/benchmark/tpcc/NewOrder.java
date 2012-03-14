package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
			New_order no;
			Order o;
			Order_line ol;
			Item it;
			Stock st;
			String district_id;
			String customer_id;
			NURand nu;
			
			String O_ID;
			String O_D_ID;
			String O_W_ID;
			String O_C_ID;
			Date O_ENTRY_D = new Date();
			String O_CARRIER_ID;
			int O_OL_CNT = 123; /*TODO*/
			int O_ALL_LOCAL;
			int OL_QUANTITY;


			int x;
			
			int ol_cnt = rand.nextInt(15-5)+5; 			
			
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
			
			no = new New_order("NO_W_"+wh.getW_ID()+"_NO_D_"+dis.getD_ID()+"_NO_O_"+dis.getD_NEXT_O());
			no.setNO_W_ID(wh.getW_ID());
			no.setNO_D_ID(dis.getD_ID());
			no.setNO_O_ID(Integer.toString(dis.getD_NEXT_O()));
			create(no);

			o = new Order("O_W_"+wh.getW_ID()+"_O_D_"+dis.getD_ID()+"_O_"+dis.getD_NEXT_O());
			o.setO_C_ID(cus.getC_ID());
			o.setO_D_ID(dis.getD_ID());
			o.setO_W_ID(wh.getW_ID());
			o.setO_ID(Integer.toString(dis.getD_NEXT_O()));
			o.setO_CARRIER_ID(null);
			o.setO_ALL_LOCAL(1);
			o.setO_OL_CNT(O_OL_CNT);
			create(o);
			
			dis.setD_NEXT_O(dis.getD_NEXT_O()+1);
			write(dis);
			
			/*for each item in this new order*/
			for(i=0; i<ol_cnt; i++){
				int OL_I_ID;
				/*
				 * A fixed 1% of the NewOrder transactions are chosen at random to simulate user data 
				 * entry errors and exercise the performance of rolling back update transactions.
	 			 */
				int rbk = rand.nextInt(100-1)+1;
				nu = new NURand(8191, 1, 100000);
				OL_I_ID = nu.calculate();
				it = read(Item.class, "I_"+OL_I_ID);
				st = read(Stock.class, "S_W_"+wh.getW_ID()+"_S_I_"+OL_I_ID);
				OL_QUANTITY = rand.nextInt(10-1)+1;
				if( i == (ol_cnt-1) && rbk == 1){
					/*roll back*/
					
					
				}
				else{	
					if(st.getS_QUANTITY() >= OL_QUANTITY+10){
						st.setS_QUANTITY(st.getS_QUANTITY() - OL_QUANTITY);
					}
					else{
						st.setS_QUANTITY(st.getS_QUANTITY() - OL_QUANTITY + 91);
					}
					
					st.setS_YTD(st.getS_YTD()+OL_QUANTITY);
					st.setS_ORDER_CNT(st.getS_ORDER_CNT()+1);
					/*TODO
					 * OL_N UMBER is set to a unique value within all the ORDER-LINE rows that have the same OL_O_ID value
					 */
					ol= new Order_line("OL_W_"+wh.getW_ID()+"_OL_D_"+dis.getD_ID()+"_OL_O_"+o.getO_ID()+"_OL_"+number);
					ol.setOL_AMOUNT(OL_QUANTITY*it.getI_PRICE());
					Pattern p = Pattern.compile("*ORIGINAL*");
					Matcher m1 = p.matcher(it.getI_DATA());
					Matcher m2 = p.matcher(st.getS_DATA());
					if(m1.find() && m2.find()) {
						/*the brand-generic field for that item is set to "B"*/
	
					}
					else{
						/*otherwise, the brand-generic field is set to "G"*/
					}
					
					
					/*TODO
					 * verify which district should put in here
					 */
					ol.setOL_DIST_INFO(st.getS_DIST_01());
					ol.setOL_DELIVERY_D(null);
					ol.setOL_I_ID(it.getI_ID());
					ol.setOL_QUANTITY(OL_QUANTITY);
					ol.setOL_W_ID(wh.getW_ID());
					ol.setOL_D_ID(dis.getD_ID());
					create(ol);
				}
			}
			
			return commitTransaction();			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}		
	}

}
