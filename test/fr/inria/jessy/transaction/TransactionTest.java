/**
 * 
 */
package fr.inria.jessy.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory.TransactionState;

/**
 * @author Masoud Saeida Ardekani
 * This is a simple test case for transaction.
 *
 */
public class TransactionTest {

	LocalJessy jessy;
	SampleTransaction st;
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

		// First, we have to define the entities read or written inside the transaction
		jessy.addEntity(SampleEntityClass.class);
		jessy.addEntity(Sample2EntityClass.class);
		
		st=new SampleTransaction(jessy);
	}

	/**
	 * Test method for {@link fr.inria.jessy.transaction.Transaction#Transaction(fr.inria.jessy.Jessy, fr.inria.jessy.transaction.TransactionHandler)}.
	 */
	@Test
	public void testTransaction() {
		ExecutionHistory result=st.execute();
		assertEquals("Result", TransactionState.COMMITTED, result.getTransactionState());

	}

	/**
	 * Test method for {@link fr.inria.jessy.transaction.Transaction#execute()}.
	 */
	@Test
	public void testExecute() {
		
		fail("Not yet implemented");
	}

}
