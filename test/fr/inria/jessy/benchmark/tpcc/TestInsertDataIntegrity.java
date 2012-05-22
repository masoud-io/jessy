package fr.inria.jessy.benchmark.tpcc;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.benchmark.tpcc.entities.Customer;
import fr.inria.jessy.benchmark.tpcc.entities.District;
import fr.inria.jessy.benchmark.tpcc.entities.History;
import fr.inria.jessy.benchmark.tpcc.entities.Item;
import fr.inria.jessy.benchmark.tpcc.entities.New_order;
import fr.inria.jessy.benchmark.tpcc.entities.Order;
import fr.inria.jessy.benchmark.tpcc.entities.Order_line;
import fr.inria.jessy.benchmark.tpcc.entities.Stock;
import fr.inria.jessy.benchmark.tpcc.entities.Warehouse;

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

	LocalJessy jessy;

	@Before
	public void setUp() throws Exception {
		jessy = LocalJessy.getInstance();

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

		Warehouse wh = jessy.read(Warehouse.class, "W_1");
		assertNotNull(wh);

		for (int i = 1; i <= 10; i++) {
			District dis = jessy.read(District.class, "D_W_1_D_" + i);

			System.out.println(dis.getLocalVector().getSelfKey());
			assertNotNull(dis);
		}

		jessy.close(this);
	}

}
