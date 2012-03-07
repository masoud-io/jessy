package fr.inria.jessy.benchmark.tpcc;


import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.SampleTransaction;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionState;


/**
 * @author WANG Haiyun & ZHAO Guang
 * 
 */
public class TpccTestInsertData {

	LocalJessy jessy;
	InsertData id;
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
	}

	/**
	 * Test method for {@link fr.inria.jessy.BenchmarkTpcc.InsertData}.
	 */
	@Test
	public void testInsertData() {
		ExecutionHistory result=id.execute();
		assertEquals("Result", TransactionState.COMMITTED, result.getTransactionState());
		
		
	}


}
