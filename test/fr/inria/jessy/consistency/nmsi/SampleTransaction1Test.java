/**
 * 
 */
package fr.inria.jessy.consistency.nmsi;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;

/**
 * @author msaeida
 *
 */
public class SampleTransaction1Test {

	LocalJessy jessy;
	SampleTransaction1 st1;
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
		
		st1=new SampleTransaction1(jessy);
	}

	/**
	 * Test method for {@link fr.inria.jessy.transaction.Transaction#Transaction(fr.inria.jessy.Jessy, fr.inria.jessy.transaction.TransactionHandler)}.
	 */
	@Test
	public void testTransaction() {
		boolean result=st1.execute();
		assertEquals("Result", true, result);

	}

}
