package fr.inria.jessy.benchmark.tpcc;

import java.util.*;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.Customer;
import fr.inria.jessy.benchmark.tpcc.entities.District;
import fr.inria.jessy.benchmark.tpcc.entities.History;
import fr.inria.jessy.benchmark.tpcc.entities.Warehouse;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class Payment extends Transaction {

	private String W_ID;
	private String D_ID;
	private String C_W_ID;
	private String C_D_ID;
	private String C_ID;
	private String C_LAST;
	private double H_AMOUNT;
	
	private int warhouseNumber;

	public Payment(Jessy jessy, int warhouseNumber) throws Exception {
		super(jessy);
		this.warhouseNumber = warhouseNumber;
	}

	@Override
	public ExecutionHistory execute() {

		try {
			Warehouse warehouse;
			District district;
			Customer customer;
			History history;
			NURand nur;
			int random;

			String[] lastnames = { "BAR", "OUGHT", "ABLE", "PRI", "PRES",
					"ESE", "ANTI", "CALLY", "ATION", "EING" };

			Random rand = new Random(System.currentTimeMillis());
			int x = rand.nextInt(100) + 1; /*
												 * x determines local or remote
												 * warehouse
												 */
			int y = rand.nextInt(100) + 1; /*
												 * y determines by C_LAST or by
												 * C_ID
												 */

			W_ID = Integer.toString(this.warhouseNumber); /* warehouse number (W_ID) is constant */
			H_AMOUNT = (float) (((float) rand.nextInt(500000 - 1) + 100) / 100.00); /* Clause 2.5.1.3(H_AMOUNT)is random within[1.00.. 5,000.00* ]*/

			/* Selection in the Warehouse table */
			warehouse = read(Warehouse.class, "W_" + W_ID);
			warehouse.setW_YTD(warehouse.getW_YTD() + this.H_AMOUNT); /*
																	 * increase
																	 * warehouse
																	 * 's
																	 * year-to
																	 * -date by
																	 * H_AMOUNT
																	 */
			/* Update Warehouse */
			write(warehouse);

			D_ID = Integer.toString(rand.nextInt(10) + 1); /*
																 * The district
																 * number (D_ID)
																 * is randomly
																 * selected
																 * within [1
																 * ..10]
																 */
			/* Selection in the District table */
			district = read(District.class, "D_W_" + W_ID + "_" + "D_" + D_ID);
			district.setD_YTD(district.getD_YTD() + this.H_AMOUNT); /*
																	 * increase
																	 * district's
																	 * year-to-date by
																	 * H_AMOUNT
																	 */
			/* Update District */
			write(district);

			if (x <= 85) { /* local warehouse */
				C_D_ID = this.D_ID;
				C_W_ID = this.W_ID;
			} else { /* remote warehouse */
			
				C_D_ID = Integer.toString(rand.nextInt(10) + 1); /* C_D_ID is randomly selected within [1.. 10] */
				while (true) {
					C_W_ID = Integer.toString(rand.nextInt(10) + 1); /* not sure ! */
					if (C_W_ID != this.W_ID) /* different to local warehouse ID 1 */
						break;
				}
				
				System.out.println("Remote Warhouse \n");
				return commitTransaction();

			}


			/* Selection Customer */
			if (y > 60) { /* by C_ID */
				nur = new NURand(1023, 1, 3000);
				C_ID = Integer.toString(nur.calculate()); /* generate C_ID */
				customer = read(Customer.class, "C_W_" + C_W_ID + "_" + "C_D_"
						+ C_D_ID + "_" + "C_" + C_ID);
				customer.setC_BALANCE(customer.getC_BALANCE() - this.H_AMOUNT);
				customer.setC_YTD_PAYMENT(customer.getC_YTD_PAYMENT()
						+ this.H_AMOUNT);
				customer.setC_PAYMENT_CNT(customer.getC_PAYMENT_CNT() + 1);
			} else { /* by C_LAST */

				nur = new NURand(255, 0, 999);
				random = nur.calculate();

				C_LAST = lastnames[random / 100]
						+ lastnames[(random % 100) / 10]
						+ lastnames[random % 10]; /* generate C_LAST */

				/* retrieve with SK C_LAST */
				Collection<Customer> collection = null;
				ReadRequestKey<String> request_C_W_ID = new ReadRequestKey<String>(
						"C_W_ID", C_W_ID);
				ReadRequestKey<String> request_C_D_ID = new ReadRequestKey<String>(
						"C_D_ID", C_D_ID);
				ReadRequestKey<String> request_C_LAST = new ReadRequestKey<String>(
						"C_LAST", C_LAST);

				List<ReadRequestKey<?>> request = new ArrayList<ReadRequestKey<?>>();
				request.add(request_C_W_ID);
				request.add(request_C_D_ID);
				request.add(request_C_LAST);
				collection = read(Customer.class, request);
				if (collection.size() == 0) {
					System.out.println("OPSSSSS: ***  "
							+ request_C_W_ID.getKeyValue() + ":"
							+ request_C_D_ID.getKeyValue() + ":"
							+ request_C_LAST.getKeyValue());
					System.exit(0);
				} 

				// int resultSize = collection.size();

				List<Customer> list = new ArrayList<Customer>();

				/* Save the results in a List for sort */
				list.addAll(collection);

				if (list.size() > 1) { /* if more than one results */

					/* Sort the results by C_FIRST in ascending order */
					for (int i = 0; i < list.size(); i++) {

						int smallest = i;
						int j;
						for (j = i; j < list.size(); j++) {

							if (list.get(j).getC_FIRST()
									.compareTo(list.get(smallest).getC_FIRST()) < 0) {
								smallest = j;
							}
						}
						swap(list, i, smallest);

					}
				}

				/* the row at position n/2 of the results set is selected */
				customer = list.get(list.size()/ 2);

				customer.setC_BALANCE(customer.getC_BALANCE() - this.H_AMOUNT);
				customer.setC_YTD_PAYMENT(customer.getC_YTD_PAYMENT()
						+ this.H_AMOUNT);
				customer.setC_PAYMENT_CNT(customer.getC_PAYMENT_CNT() + 1);
			}

			if (customer.getC_Credit() == "BC") {
				String data = "C_" + customer.getC_ID() + "_" + "C_D_"
						+ customer.getC_D_ID() + "_" + "C_W_"
						+ customer.getC_W_ID() + "_" + "D_"
						+ district.getD_ID() + "_" + "W_" + warehouse.getW_ID()
						+ "_" + this.H_AMOUNT + "_" + customer.getC_DATA();
				if (data.length() > 500) {
					data = data.substring(0, 499); /*
													 * C_DATA field never
													 * exceeds 500 characters
													 */
				}
				customer.setC_DATA(data);
			}

			/* Update Customer */
			write(customer);

			String key = "H_C_W_" + this.C_W_ID + "_" + "H_C_D_" + this.C_D_ID
					+ "_" + "H_C_" + this.C_ID;
			history = new History(key);
			history.setH_D_ID("H_D_" + this.D_ID);
			history.setH_W_ID("H_W_" + this.W_ID);
			history.setH_AMOUNT(this.H_AMOUNT);
			history.setH_DATA(warehouse.getW_NAME() + "    "
					+ district.getD_NAME());
			/* Insertion History */
			write(history);

			return commitTransaction();

		} catch (Exception e) {
			e.printStackTrace();
			return abortTransaction();
		}

	}

	public void swap(List<Customer> list, int from, int to) {

		Customer tmp = list.get(from);
		list.set(from, list.get(to));
		list.set(to, tmp);

	}

}
