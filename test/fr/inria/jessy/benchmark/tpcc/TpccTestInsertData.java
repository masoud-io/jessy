package fr.inria.jessy.benchmark.tpcc;


import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.benchmark.tpcc.entities.*;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.*;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionState;


/**
 * @author WANG Haiyun & ZHAO Guang
 * 
 */
public class TpccTestInsertData{

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
		
		jessy.addEntity(Warehouse.class);
		jessy.addEntity(District.class);
		jessy.addEntity(Customer.class);
		jessy.addEntity(Item.class);
		id=new InsertData(jessy);
		
	}

	/**
	 * Test method for {@link fr.inria.jessy.BenchmarkTpcc.InsertData}.
	 * @throws Exception 
	 */
	@Test
	public void testInsertData() throws Exception {
		try {
			ExecutionHistory result=id.execute();
			/*test execution*/
			assertEquals("Result", TransactionState.COMMITTED, result.getTransactionState());
		/*
			/*test inserted what we expected 
			wh = jessy.read(Warehouse.class, "W_1");
			assertEquals("Warehouse name", "Warehouse1", wh.getW_ID());
			di = jessy.read(District.class, "W_1_D_1");
			assertEquals("District name", "District1", di.getD_ID());
		*/
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}


}
