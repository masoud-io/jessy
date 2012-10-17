package fr.inria.jessy.benchmark.tpcc.workflow;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.JessyFactory;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.benchmark.tpcc.Delivery;
import fr.inria.jessy.benchmark.tpcc.NewOrder;
import fr.inria.jessy.benchmark.tpcc.OrderStatus;
import fr.inria.jessy.benchmark.tpcc.Payment;
import fr.inria.jessy.benchmark.tpcc.StockLevel;
import fr.inria.jessy.benchmark.tpcc.entities.Customer;
import fr.inria.jessy.benchmark.tpcc.entities.District;
import fr.inria.jessy.benchmark.tpcc.entities.History;
import fr.inria.jessy.benchmark.tpcc.entities.Item;
import fr.inria.jessy.benchmark.tpcc.entities.New_order;
import fr.inria.jessy.benchmark.tpcc.entities.Order;
import fr.inria.jessy.benchmark.tpcc.entities.Order_line;
import fr.inria.jessy.benchmark.tpcc.entities.Stock;
import fr.inria.jessy.benchmark.tpcc.entities.Warehouse;

public class TpccClient {

	Jessy jessy;

	int numberTransaction; /* number of Transactions to execute */
	int warehouseNumber;
	int districtNumber;

	int quotient = 0; /* for the number of NewOrder and Payment Transactions */
	int rest = 0;
	int i, j;

	private static Logger logger = Logger.getLogger(TpccClient.class);

	public TpccClient(int numberTransaction, int warehouseNumber, int districtNumber) {
		PropertyConfigurator.configure("log4j.properties");

		this.numberTransaction = numberTransaction;
		this.warehouseNumber = warehouseNumber;
		this.districtNumber = districtNumber;

		try {
//			jessy = JessyFactory.getDistributedJessy();
			jessy = JessyFactory.getLocalJessy();

			jessy.addEntity(Warehouse.class);
			jessy.addEntity(District.class);
			jessy.addEntity(Customer.class);
			jessy.addEntity(Item.class);
			jessy.addEntity(Stock.class);
			jessy.addEntity(History.class);
			jessy.addEntity(Order.class);
			jessy.addEntity(New_order.class);
			jessy.addEntity(Order_line.class);

			jessy.addSecondaryIndex(Customer.class, String.class, "C_W_ID");
			jessy.addSecondaryIndex(Customer.class, String.class, "C_D_ID");
			jessy.addSecondaryIndex(Customer.class, String.class, "C_LAST");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void execute() {

		/*
		 * System.out.print("Input the number of transactions : "); Scanner
		 * reader = new Scanner(System.in); String s = reader.nextLine(); int
		 * number = (int) Integer.valueOf(s);
		 */

		quotient = 10 * (numberTransaction / 23);
		rest = numberTransaction % 23;

		try {
			if (this.numberTransaction < 23) {

				/* for the rest */
				for (i = 1; i <= rest / 2; i++) {

					neworder();
					logger.debug("1 NewOrder transaction committed in round: " + i );

					payment();
					logger.debug("2 Payment transaction committed in round: " + i );
				}

				if (rest % 2 == 1) {

					neworder();
					logger.debug("3 NewOrder transaction committed in round: " + i );
				}

			} else {

				for (i = 1; i <= quotient; i++) {

					neworder();
					logger.debug("4 NewOrder transaction committed in round: " + i );

					payment();
					logger.debug("5 Payment transaction committed in round: " + i );

					/* for decks */
					if (i != 0 && i % 10 == 0) {

						orderstatus();
						logger.debug("6 OrderStatus transaction committed in round: " + i );

						delivery();
						logger.debug("7 Delivery transaction committed in round: " + i );

						stocklevel();
						logger.debug("8 StockLevel transaction committed in round: " + i );
					}

				}

				/* for the rest */
				for (j = 0; j < rest / 2; j++) {

					neworder();
					logger.debug("9 NewOrder transaction committed in round: " + i );

					payment();
					logger.debug("10 Payment transaction committed in round: " + i );
				}

				if (rest % 2 == 1) {

					neworder();
					logger.debug("11 NewOrder transaction committed in round: " + i );
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void neworder() throws Exception {

		logger.debug("executing NewOrder...");
		NewOrder neworder = new NewOrder(jessy, this.warehouseNumber);
		neworder.execute();
	}

	public void payment() throws Exception {
		
		logger.debug("executing payment...");
		Payment payment = new Payment(jessy, this.warehouseNumber);
		payment.execute();
	}

	public void orderstatus() throws Exception {
		
		logger.debug("executing orderstatus...");
		OrderStatus orderstatus = new OrderStatus(jessy, this.warehouseNumber);
		orderstatus.execute();
	}

	public void delivery() throws Exception {

		logger.debug("executing delivery...");
		Delivery delivery = new Delivery(jessy, this.warehouseNumber);
		delivery.execute();
	}

	public void stocklevel() throws Exception {

		logger.debug("executing stocklevel...");
		StockLevel stocklevel = new StockLevel(jessy, this.warehouseNumber, this.districtNumber);
		stocklevel.execute();
	}

	public static void main(String[] args) throws Exception {

		TpccClient client = new TpccClient(4096, 10, 10);
		client.execute();

//		TODO cleanly close fractal
		System.exit(0);
	}

}
