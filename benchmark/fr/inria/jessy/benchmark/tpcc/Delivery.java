package fr.inria.jessy.benchmark.tpcc;

import java.util.Date;
import java.util.Random;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.Customer;
import fr.inria.jessy.benchmark.tpcc.entities.District;
import fr.inria.jessy.benchmark.tpcc.entities.New_order;
import fr.inria.jessy.benchmark.tpcc.entities.Order;
import fr.inria.jessy.benchmark.tpcc.entities.Order_line;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;


public class Delivery extends Transaction {

	
	private String W_ID;
	private String O_CARRIER_ID;

	private int noNumber;

	public Delivery(Jessy jessy, int warhouseNumber) throws Exception {
		super(jessy);
		W_ID= ""+warhouseNumber;
	}

	@Override
	public ExecutionHistory execute() {

		try {
			District district;
			New_order no = null;
			Order order;
			Order_line ol = null;
			Customer customer;
			int i, j, k;
			int order_counter = 0;
			/*
			 * File log = new File("log file"); FileWriter fw = new
			 * FileWriter(log);
			 */

			Random rand = new Random(System.currentTimeMillis());

			/* for each of the 10 districts within the given warehouse */
			for (i = 1; i <= 10; i++) {
				district = read(District.class, "D_W_" + W_ID + "_" + "D_" + i);
//				TODO implement in a more efficient way if possible...
				/* retrieve the oldest undelivered order of the district */
				for (j = 1; j < district.getD_NEXT_O(); j++) {
					no = read(New_order.class, "NO_W_" + W_ID + "_" + "NO_D_"
							+ i + "_" + "NO_O_" + j);
					if (no != null) {
						noNumber = j;

						/*
						 * find the result and skip the loop
						 */
						j = district.getD_NEXT_O();
						
//						break;
					}
//					TODO fixme
					break;
				}

				if (no == null) { /* no result, skip */
					/*
					 * TODO is this right? record the result in the log file,
					 * but not necessary for the moment according to Masoud
					 */
					// fw.write("no matching result in"+ i + "district\n");
					continue;
				}
				/* increment the undelivered order number */
				order_counter++;
				/*
				 * save the NO_O_ID which will be used later before delete the
				 * row
				 */
				// int NO_O_ID = no.getNO_O_ID();

				/* Delete at New_Order */
				no.setNO_W_ID(null);
				no.setNO_D_ID(null);
				no.setNO_O_ID(0);
				write(no);

				order = read(Order.class, "O_W_" + W_ID + "_" + "O_D_" + i
						+ "_" + "O_" + noNumber);

				/*
				 * The carrier number ( O_CARRIER_ID ) is randomly selected
				 * within [1 .. 10]
				 */
				O_CARRIER_ID = Integer.toString(rand.nextInt(10) + 1);
				order.setO_CARRIER_ID(O_CARRIER_ID);

				/* Update Order */
				write(order);

				int total_amount = 0;
				for (k = 1; k <= order.getO_OL_CNT(); k++) {
					ol = read(Order_line.class, "OL_W_" + W_ID + "_" + "OL_D_"
							+ i + "_" + "OL_O_" + order.getO_ID() + "_" + "OL_"
							+ k);
					ol.setOL_DELIVERY_D(new Date());
					total_amount += ol.getOL_AMOUNT();
				}
				/* Update Order_line */
				write(ol);

				customer = read(Customer.class, "C_W_" + W_ID + "_" + "C_D_"
						+ i + "_" + "C_" + order.getO_C_ID());
				customer.setC_BALANCE(customer.getC_BALANCE() + total_amount);
				customer.setC_DELIVERY_CNT(customer.getC_DELIVERY_CNT() + 1);
				/* Update Customer */
				write(customer);

				// fw.close();
			}
			/*
			 * The transaction is committed unless more orders will be delivery
			 * within this transaction
			 */
			if (order_counter > 1){
				return commitTransaction();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return abortTransaction();
	}

}
