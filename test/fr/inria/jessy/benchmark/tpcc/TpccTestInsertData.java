package fr.inria.jessy.benchmark.tpcc;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.transaction.*;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionState;

/**
 * @author WANG Haiyun & ZHAO Guang
 * 
 */
public class TpccTestInsertData {

	LocalJessy jessy;
	InsertData id;
	TpccVerifyInsertedData vid;
	Warehouse wh;
	District di;
	Item it;
	Customer cu;

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
		id = new InsertData(jessy);
		vid = new TpccVerifyInsertedData(jessy);

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

		/* test inserted what we expected */
		ExecutionHistory result1 = vid.execute();
		assertEquals("Result1", TransactionState.COMMITTED,
				result1.getTransactionState());

		wh = vid.getW();
		di = vid.getD();
		cu = vid.getC();
		it = vid.getI();

		assertEquals("Warehouse id", "W_1", wh.getW_ID());
		assertEquals("District id", "D_1", di.getD_ID());
		assertEquals("customer id", "C_1", cu.getC_ID());
		assertEquals("item id", "I_1", it.getI_ID());

	}

}
