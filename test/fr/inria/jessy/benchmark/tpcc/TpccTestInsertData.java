package fr.inria.jessy.benchmark.tpcc;


import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.store.*;
import fr.inria.jessy.transaction.*;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionState;


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
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		jessy=LocalJessy.getInstance();
		
		id=new InsertData(jessy);
		wh = new Warehouse();
		di = new District();
		it = new Item();
		cu = new Customer();
		
	}

	/**
	 * Test method for {@link fr.inria.jessy.BenchmarkTpcc.InsertData}.
	 */
	@Test
	public void testInsertData() {
		ExecutionHistory result=id.execute();
		/*test execution*/
		assertEquals("Result", TransactionState.COMMITTED, result.getTransactionState());
	
		/*test inserted what we expected */
		wh = read(Warehouse.class, "W_1");
	}


}
