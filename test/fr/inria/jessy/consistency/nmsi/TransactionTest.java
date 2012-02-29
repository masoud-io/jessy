/**
 * 
 */
package fr.inria.jessy.consistency.nmsi;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase;

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
public class TransactionTest extends TestCase {

	LocalJessy jessy;

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

		// First, we have to define the entities read or written inside the
		// transaction
		jessy.addEntity(SampleEntityClass.class);
		jessy.addEntity(Sample2EntityClass.class);

	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.transaction.Transaction#Transaction(fr.inria.jessy.Jessy, fr.inria.jessy.transaction.TransactionHandler)}
	 * .
	 * @throws Exception 
	 */
	@Test
	public void testTransaction() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(2);

		Future<Boolean> future1;
		future1 = pool.submit(new SampleInitTransaction(jessy));

		Future<Boolean> future2;
		future2 = pool.submit(new SampleTransaction2(jessy));

		Boolean result1=future1.get();
		Boolean result2=future2.get();
		

		assertEquals("Result", true, result1 & result2);

	}

}
