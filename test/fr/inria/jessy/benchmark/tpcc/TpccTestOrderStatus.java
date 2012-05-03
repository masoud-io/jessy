package fr.inria.jessy.benchmark.tpcc;

import static org.junit.Assert.*;

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

public class TpccTestOrderStatus {
	
	LocalJessy jessy;
	OrderStatus os; 


	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
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
		
		jessy.addSecondaryIndex(Customer.class, String.class, "C_W_ID");
		jessy.addSecondaryIndex(Customer.class, String.class, "C_D_ID");
		jessy.addSecondaryIndex(Customer.class, String.class, "C_LAST");
		os = new OrderStatus(jessy, 1);
	}
	
	/**
	 * Test method for {@link fr.inria.jessy.BenchmarkTpcc.InsertData}.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testOrderStatus() throws Exception {

		ExecutionHistory result = os.execute();
		/* test execution */
		assertEquals("Result", TransactionState.COMMITTED,
				result.getTransactionState());			

	}

}
