/**
 * 
 */
package fr.inria.jessy.distributed.nmsi;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.distributed.nmsi.transaction.SampleEntityInitTransaction;
import fr.inria.jessy.distributed.nmsi.transaction.SampleTransactionMultiObj1;
import fr.inria.jessy.distributed.nmsi.transaction.SampleTransactionMultiObj2;
import fr.inria.jessy.distributed.nmsi.transaction.SampleTransactionMultiObj3;
import fr.inria.jessy.distributed.nmsi.transaction.SampleTransactionMultiObj4;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionState;

/**
 * @author Masoud Saeida Ardekani This a test class for checking NMSI in a
 *         multi-object scenario.
 *         <p>
 *         The scenario is as follows: {@link SampleEntityInitTransaction} and
 *         {@link Sample2EntityInitTransaction} initialize two obejcts.
 *         {@link SampleTransactionMultiObj1} reads and writes on the first
 *         object. {@link SampleTransactionMultiObj3} reads and writes on the
 *         second object. {@link SampleTransactionMultiObj3} reads the initial
 *         values of two objects and writes after all transaction has been
 *         committed. Therefore, {@link SampleTransactionMultiObj3} should abort
 *         by the certification test, and the other should commit. There is also
 *         one read only transaction {@link SampleTransactionMultiObj4} that
 *         first reads the initial value for {@link SampleEntityClass} and when
 *         all update transaction have committed, reads
 *         {@link Sample2EntityClass}
 */
public class MultiObjNMSITest extends TestCase {

	DistributedJessy jessy;

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
		PropertyConfigurator.configure("log4j.properties");
		jessy = DistributedJessy.getInstance();

		// First, we have to define the entities read or written inside the
		// transaction
		jessy.addEntity(SampleEntityClass.class);
		jessy.addEntity(Sample2EntityClass.class);

	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.transaction.Transaction#Transaction(fr.inria.jessy.Jessy, fr.inria.jessy.transaction.TransactionHandler)}
	 * .
	 * 
	 * @throws Exception
	 */
	@Test
	public void testTransaction() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(4);

		Future<ExecutionHistory> futureInit1;
		futureInit1 = pool.submit(new SampleEntityInitTransaction(jessy));

//		Future<ExecutionHistory> futureInit2;
//		futureInit2 = pool.submit(new Sample2EntityInitTransaction(jessy));

		Future<ExecutionHistory> future1;
		future1 = pool.submit(new SampleTransactionMultiObj1(jessy));

		Future<ExecutionHistory> future2;
		future2 = pool.submit(new SampleTransactionMultiObj2(jessy));

		Future<ExecutionHistory> future3;
		future3 = pool.submit(new SampleTransactionMultiObj3(jessy));

		Future<ExecutionHistory> future4;
		future4 = pool.submit(new SampleTransactionMultiObj4(jessy));

		ExecutionHistory resultInit1 = futureInit1.get();
		assertEquals("Result", TransactionState.COMMITTED,
				resultInit1.getTransactionState());

//		ExecutionHistory resultInit2 = futureInit2.get();
//		assertEquals("Result", TransactionState.COMMITTED,
//				resultInit2.getTransactionState());

		ExecutionHistory result1 = future1.get();
		assertEquals("Result", TransactionState.COMMITTED,
				result1.getTransactionState());

		ExecutionHistory result2 = future2.get();
		assertEquals("Result", TransactionState.ABORTED_BY_CERTIFICATION,
				result2.getTransactionState());

		ExecutionHistory result3 = future3.get();
		assertEquals("Result", TransactionState.COMMITTED,
				result3.getTransactionState());

		ExecutionHistory result4 = future4.get();
		assertEquals("Result", TransactionState.COMMITTED,
				result4.getTransactionState());

	}

}
