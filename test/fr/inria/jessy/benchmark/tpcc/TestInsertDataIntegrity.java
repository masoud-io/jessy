package fr.inria.jessy.benchmark.tpcc;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.JessyFactory;
import fr.inria.jessy.benchmark.tpcc.entities.Customer;
import fr.inria.jessy.benchmark.tpcc.entities.District;
import fr.inria.jessy.benchmark.tpcc.entities.History;
import fr.inria.jessy.benchmark.tpcc.entities.Item;
import fr.inria.jessy.benchmark.tpcc.entities.New_order;
import fr.inria.jessy.benchmark.tpcc.entities.Order;
import fr.inria.jessy.benchmark.tpcc.entities.Order_line;
import fr.inria.jessy.benchmark.tpcc.entities.Stock;
import fr.inria.jessy.benchmark.tpcc.entities.Warehouse;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

/**
 * This class tests whether {@code InsertData#execute()} has been executed
 * properly and all data are permanent or not.
 * 
 * TODO finish this test case.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class TestInsertDataIntegrity extends TestCase {

//	LocalJessy jessy;
	Jessy jessy;

	@Before
	public void setUp() throws Exception {
		jessy = JessyFactory.getLocalJessy();

		jessy.registerClient(this);

		jessy.addEntity(Warehouse.class);
		jessy.addEntity(District.class);
		jessy.addEntity(Customer.class);
		jessy.addEntity(Item.class);
		jessy.addEntity(Stock.class);
		jessy.addEntity(History.class);
		jessy.addEntity(Order.class);
		jessy.addEntity(New_order.class);
		jessy.addEntity(Order_line.class);
	}

	/**
	 * 
	 * @throws Exception
	 */
	@Test
	public void testInsertDataIntegrity() throws Exception {
		Transaction myTran = new Transaction(jessy) {

			@Override
			public ExecutionHistory execute() {
				Warehouse wh;
				try {
					wh = read(Warehouse.class, "W_1");
					assertNotNull(wh);
					
					for (int i = 1; i <= 10; i++) {
						District dis = read(District.class, "D_W_1_D_"
								+ i);
						assertNotNull(dis);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return commitTransaction();
			}

		};

		ExecutionHistory eh=myTran.execute();
		for(JessyEntity je:eh.getReadSet().getEntities()){
			System.out.println(je.getLocalVector().getSelfKey());
		}
		assertTrue(true);
		jessy.close(this);
	}

}
