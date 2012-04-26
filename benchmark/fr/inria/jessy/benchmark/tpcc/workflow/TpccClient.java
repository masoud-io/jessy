package fr.inria.jessy.benchmark.tpcc.workflow;

import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.mortbay.log.Log;

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

	LocalJessy jessy;

	NewOrder neworder;
	Payment payment;
	OrderStatus orderstatus;
	Delivery delivery;
	StockLevel stocklevel;

	int quotient = 0; /* for the number of NewOrder and Payment transactions */
	int rest = 0;
	int i, j;

	private static Logger logger = Logger.getLogger(TpccClient.class);

	public TpccClient() {
		PropertyConfigurator.configure("log4j.properties");

		try {
			jessy = LocalJessy.getInstance();

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

		System.out.print("Input the number of transactions : ");
		Scanner reader = new Scanner(System.in);
		String s = reader.nextLine();
		int number = (int) Integer.valueOf(s);

		quotient = 10 * (number / 23);
		rest = number % 23;

		try {
			for (i = 1; i <= quotient; i++) {

				logger.debug(i);

				neworder();
				System.out.println("NewOrder transaction committed");

				payment();
				System.out.println("Payment transaction committed");

				/* for decks */
				if (i != 0 && i % 10 == 0) {

					orderstatus();
					System.out.println("OrderStatus transaction committed");

					delivery();
					System.out.println("Delivery transaction committed");

					stocklevel();
					System.out.println("StockLevel transaction committed");
				}

				/* for the rest */
				for (j = 0; j < rest / 2; j++) {

					System.out.println(j);

					neworder();
					System.out.println("NewOrder transaction committed");

					payment();
					System.out.println("Payment transaction committed");
				}

				if (rest % 2 == 1) {
					System.out.println(j + 1);
					neworder();
					System.out.println("NewOrder transaction committed");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void neworder() throws Exception {

		neworder = new NewOrder(jessy);
		neworder.execute();
	}

	public void payment() throws Exception {

		payment = new Payment(jessy);
		payment.execute();
	}

	public void orderstatus() throws Exception {

		orderstatus = new OrderStatus(jessy);
		orderstatus.execute();
	}

	public void delivery() throws Exception {

		delivery = new Delivery(jessy);
		delivery.execute();
	}

	public void stocklevel() throws Exception {

		stocklevel = new StockLevel(jessy);
		stocklevel.execute();
	}

	public static void main(String[] args) throws Exception {

		TpccClient client = new TpccClient();
		client.execute();

	}

}
