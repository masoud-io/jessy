/**
 * 
 */
package fr.inria.jessy.consistency.distributed.psi;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.consistency.distributed.nmsi.transaction.SampleEntityInitTransaction;
import fr.inria.jessy.consistency.distributed.nmsi.transaction.SampleTransactionMultiObj1;
import fr.inria.jessy.consistency.distributed.nmsi.transaction.SampleTransactionMultiObj2;
import fr.inria.jessy.consistency.distributed.nmsi.transaction.SampleTransactionMultiObj3;
import fr.inria.jessy.consistency.distributed.nmsi.transaction.SampleTransactionMultiObj4;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;
import fr.inria.jessy.transaction.TransactionState;

/**
 * @author Masoud Saeida Ardekani This a test class for checking NMSI in a
 *         multi-object scenario.
 */
public class MultiObjWritePSITest extends TestCase {

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
	 * It sets {@code retryCommitOnAbort to false. Thus transaction
	 * 
	 * @code SampleTransactionMultiObj2} must abort}
	 * 
	 * @throws Exception
	 */
	@Test
	public void testNMSI_NoRetryOnAbort() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(4);

		Future<ExecutionHistory> futureInit1;
		futureInit1 = pool.submit(new SampleEntityInitTransaction(jessy));

		ExecutionHistory resultInit1 = futureInit1.get();
		assertEquals("Result", TransactionState.COMMITTED,
				resultInit1.getTransactionState());

		Future<ExecutionHistory> future11;
		future11 = pool.submit(new WriteTransaction1(jessy));

		ExecutionHistory result11 = future11.get();
		assertEquals("Result", TransactionState.COMMITTED,
				result11.getTransactionState());

		Future<ExecutionHistory> future12;
		future12 = pool.submit(new WriteTransaction1(jessy));

		ExecutionHistory result12 = future12.get();
		assertEquals("Result", TransactionState.COMMITTED,
				result12.getTransactionState());
		
		Future<ExecutionHistory> future2;
		future2 = pool.submit(new WriteTransaction2(jessy));

		ExecutionHistory result2 = future2.get();
		assertEquals("Result", TransactionState.COMMITTED,
				result2.getTransactionState());

		
	}

	private class InitTransaction extends Transaction {

		public InitTransaction(Jessy jessy) throws Exception {
			super(jessy);
		}

		@Override
		public ExecutionHistory execute() {
			try {

				SampleEntityClass se = new SampleEntityClass("1",
						"sampleentity1_INITIAL");
				create(se);

				Sample2EntityClass se2 = new Sample2EntityClass("2",
						"sample2entity2_INITIAL");
				create(se2);

				return commitTransaction();
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}

	}

	private class WriteTransaction1 extends Transaction {

		public WriteTransaction1(Jessy jessy) throws Exception {
			super(jessy);
			setRetryCommitOnAbort(false);
		}

		@Override
		public ExecutionHistory execute() {
			try {

				Thread.sleep(500);
				SampleEntityClass se = read(SampleEntityClass.class, "1");

				se.setData("consistency depends write");
				write(se);

				return commitTransaction();
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}

	}

	private class WriteTransaction2 extends Transaction {

		public WriteTransaction2(Jessy jessy) throws Exception {
			super(jessy);
			setRetryCommitOnAbort(false);
		}

		@Override
		public ExecutionHistory execute() {
			try {

				Thread.sleep(500);

				Sample2EntityClass se2 = new Sample2EntityClass("2",
						"second write");
				write(se2);

				return commitTransaction();
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}

	}

}
