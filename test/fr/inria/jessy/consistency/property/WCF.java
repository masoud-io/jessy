package fr.inria.jessy.consistency.property;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;
import fr.inria.jessy.transaction.TransactionState;

/**
 * This test case tests all consistency criterion whether they ensure WCF or
 * not. 
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class WCF extends TestCase {
	Jessy jessy;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		jessy =new DistributedJessy();

		// First, we have to define the entities read or written inside the
		// transaction
		jessy.addEntity(SampleEntityClass.class);
		jessy.addEntity(Sample2EntityClass.class);
		Transaction.setRetryCommitOnAbort(false);
	}

	@Test
	public void testConcurrentWrite() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(4);

		Future<ExecutionHistory> futureInit1;
		futureInit1 = pool.submit(new InitTransaction(jessy));

		assertEquals(futureInit1.get().getTransactionState(),
				TransactionState.COMMITTED);

		Future<ExecutionHistory> futureWrite1;
		futureWrite1 = pool.submit(new WriteTransaction1(jessy));

		Future<ExecutionHistory> futureWrite2;
		futureWrite2 = pool.submit(new WriteTransaction2(jessy));

		assertEquals(futureWrite1.get().getTransactionState(),
				TransactionState.COMMITTED);

		assertNotSame(futureWrite2.get().getTransactionState(),
				TransactionState.COMMITTED);

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

		}

		@Override
		public ExecutionHistory execute() {
			try {

				Thread.sleep(500);

				SampleEntityClass se = read(SampleEntityClass.class, "1");
				se.setData("Second Write");
				write(se);

				return commitTransaction();
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}

	}

	/**
	 * Read before {@code WriteTransaction1}, thus must abort.
	 * 
	 */
	private class WriteTransaction2 extends Transaction {

		public WriteTransaction2(Jessy jessy) throws Exception {
			super(jessy);

		}

		@Override
		public ExecutionHistory execute() {
			try {
				SampleEntityClass se = read(SampleEntityClass.class, "1");

				Thread.sleep(1500);

				se.setData("third write");

				write(se);

				return commitTransaction();
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}

	}

}
