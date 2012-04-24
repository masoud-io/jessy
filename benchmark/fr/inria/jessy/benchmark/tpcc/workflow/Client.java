package fr.inria.jessy.benchmark.tpcc.workflow;

import java.util.*;

import fr.inria.jessy.*;
import fr.inria.jessy.benchmark.tpcc.*;
import fr.inria.jessy.benchmark.tpcc.entities.*;


public class Client {

	LocalJessy jessy;
	
	NewOrder neworder;
	Payment payment;
	OrderStatus orderstatus;
	Delivery delivery;
	StockLevel stocklevel;
	
	int quotient = 0;        /* for the number of NewOrder and Payment transactions */ 
	int rest = 0;
	int i,j;
	
	public void execute() {
		
		System.out.print("Input the number of transactions : ");
		Scanner reader = new Scanner(System.in);
		String s = reader.nextLine();
		int number = (int)Integer.valueOf(s);
		
		quotient = 10*(number/23);
		rest = number%23;
		
		try {
			for(i=1; i<=quotient; i++) {
				
				System.out.println(i);
				
				neworder();
				System.out.println("NewOrder transaction committed");
				
				payment();
				System.out.println("Payment transaction committed");
				
				/* for decks */
				if(i!=0 && i%10 == 0) {
					
					orderstatus();					
					System.out.println("OrderStatus transaction committed");
					
					delivery();
					System.out.println("Delivery transaction committed");
					
					stocklevel();
					System.out.println("StockLevel transaction committed");
				}
				
				/* for the rest */
				for(j=0; j<rest/2;j++) {
					
					System.out.println(j);
					
					neworder();
					System.out.println("NewOrder transaction committed");
					
					payment();
					System.out.println("Payment transaction committed");
				}
				
				if(rest%2==1) {
					System.out.println(j+1);
					neworder();
					System.out.println("NewOrder transaction committed");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void neworder() throws Exception {
		
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
		
		neworder = new NewOrder(jessy);
		neworder.execute();
	}
	
	public void payment() throws Exception {
		
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
		
		payment = new Payment(jessy);
		payment.execute();
	}
	
	public void orderstatus() throws Exception {
		
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
		
		orderstatus = new OrderStatus(jessy);
		orderstatus.execute();
	}
	
	public void delivery() throws Exception {
		
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
		
		delivery = new Delivery(jessy);
		delivery.execute();
	}
	
	public void stocklevel() throws Exception {
		
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
		
		stocklevel = new StockLevel(jessy);
		stocklevel.execute();
	}
	
	public static void main(String[] args) throws Exception {
		
		Client client = new Client();
		client.execute();

	}

}
