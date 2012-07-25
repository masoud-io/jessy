package fr.inria.jessy.consistency.property;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.utils.Configuration;

/**
 * This testcase tests all consistency criterion whether they ensure SCONSa or
 * not. While NMSI, SER, and RC do not ensure SCONSa, PSI and SI must ensure
 * SCONSa.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class SCONSa extends TestCase {
	DistributedJessy jessy;

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

	@Test
	public void testNMSI_NoRetryOnAbort() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(4);

		Future<ExecutionHistory> futureInit1;
		futureInit1 = pool.submit(new InitTransaction(jessy));

		Future<ExecutionHistory> futureProblematic;
		futureProblematic = pool.submit(new ReadWriteTransaction(jessy));

		Future<ExecutionHistory> futureWrite;
		futureWrite = pool.submit(new WriteTransaction(jessy));

		assertEquals(futureInit1.get().getTransactionState(),
				TransactionState.COMMITTED);

		assertEquals(futureWrite.get().getTransactionState(),
				TransactionState.COMMITTED);

		String consistency = Configuration
				.readConfig(ConstantPool.CONSISTENCY_TYPE);

		if (consistency.equals("nmsi") || consistency.equals("nmsi2")
				|| consistency.equals("ser") || consistency.equals("rc")
				|| consistency.equals("us") || consistency.equals("us2")) {
			assertEquals(futureProblematic.get().getTransactionState(),
					TransactionState.COMMITTED);
		} else if (consistency.equals("psi") || consistency.equals("si")
				|| consistency.equals("si2")) {
			assertNotSame(futureProblematic.get().getTransactionState(),
					TransactionState.COMMITTED);
		}
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

	private class ReadWriteTransaction extends Transaction {

		public ReadWriteTransaction(Jessy jessy) throws Exception {
			super(jessy);
			setRetryCommitOnAbort(false);
		}

		@Override
		public ExecutionHistory execute() {
			try {

				Thread.sleep(500);
				SampleEntityClass se = read(SampleEntityClass.class, "1");

				Thread.sleep(1500);
				Sample2EntityClass se2 = read(Sample2EntityClass.class, "2");

				Thread.sleep(2000);
				se2.setData("consistency depends write");
				write(se2);

				return commitTransaction();
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}

	}

	private class WriteTransaction extends Transaction {

		public WriteTransaction(Jessy jessy) throws Exception {
			super(jessy);

		}

		@Override
		public ExecutionHistory execute() {
			try {

				Thread.sleep(1000);

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
