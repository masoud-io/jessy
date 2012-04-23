package fr.inria.jessy.benchmark.tpcc;

import static org.junit.Assert.*;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.*;

/**
 * @author WANG Haiyun & ZHAO Guang
 * 
 */
public class TpccTestInsertData {

	LocalJessy jessy;
	InsertData id;
	Warehouse wh;
	District di;
	Item it;
	Customer cu;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		PropertyConfigurator.configure("log4j.properties");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
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
		id = new InsertData(jessy);

	}

	/**
	 * Test method for {@link fr.inria.jessy.BenchmarkTpcc.InsertData}.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testInsertData() throws Exception {

		ExecutionHistory result = id.execute();
		/* test execution */
		assertEquals("Result", TransactionState.COMMITTED,
				result.getTransactionState());

			

	}

}
