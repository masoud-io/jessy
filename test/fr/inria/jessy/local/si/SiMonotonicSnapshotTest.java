package fr.inria.jessy.local.si;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.local.si.transaction.InitTransaction;
import fr.inria.jessy.local.si.transaction.T1;
import fr.inria.jessy.local.si.transaction.T2;
import fr.inria.jessy.local.si.transaction.T3;
import fr.inria.jessy.local.si.transaction.T4;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.vector.ScalarVector;

/**
 * 
 * Check the snapshot of transactions in a scenario in which nmsi will return a
 * non-monotonic snapshot. Scenario: There are four concurrent transactions T1,
 * T2, T3 and T4. T1 and T4 start concurrently. T1 read x and T4 read y. After
 * T1 and T4 has read T2 and T3 start and commit. T2 write on x and T3 write on
 * y. When T2 and T3 are committed transactions T1 and T4 reads y and x,
 * respectively. In SI T1 and T4 must both read the initial value of x and y. In
 * nmsi they read the last committed value.
 * 
 * @author pcincilla
 * 
 */

public class SiMonotonicSnapshotTest {

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
		PropertyConfigurator.configure("log4j.properties");
		jessy = LocalJessy.getInstance();

		// First, we have to define the entities read or written inside the
		// transaction
		jessy.addEntity(SampleEntityClass.class);
		jessy.addEntity(Sample2EntityClass.class);
	}

	@Test
	public void testTransaction() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(5);

		// INIT
		Future<ExecutionHistory> futureInit;
		futureInit = pool.submit(new InitTransaction(jessy));

		ExecutionHistory result0 = futureInit.get();
		assertEquals(TransactionState.COMMITTED, result0.getTransactionState());
		assertEquals(0, ScalarVector.lastCommittedTransactionSeqNumber.get());

		Future<ExecutionHistory> future1;
		Future<ExecutionHistory> future2;
		Future<ExecutionHistory> future3;
		Future<ExecutionHistory> future4;

		future1 = pool.submit(new T1(jessy));
		future4 = pool.submit(new T4(jessy));
		future2 = pool.submit(new T2(jessy));
		future3 = pool.submit(new T3(jessy));

		ExecutionHistory result2 = future2.get();
		assertEquals(result2.getTransactionState(), TransactionState.COMMITTED);

		ExecutionHistory result3 = future3.get();
		assertEquals(result3.getTransactionState(), TransactionState.COMMITTED);

		ExecutionHistory result1 = future1.get();
		assertEquals(result1.getTransactionState(), TransactionState.COMMITTED);
		
		ExecutionHistory result4 = future4.get();
		assertEquals(result4.getTransactionState(), TransactionState.COMMITTED);

		Iterator<JessyEntity> rs1iterator = result1.getReadSet().getEntities()
				.iterator();
		Iterator<JessyEntity> rs4iterator = result4.getReadSet().getEntities()
				.iterator();

		assertEquals("0y", ((Sample2EntityClass) rs1iterator.next()).getData());
		assertEquals("0x", ((SampleEntityClass) rs1iterator.next()).getData());

		assertEquals("0y", ((Sample2EntityClass) rs4iterator.next()).getData());
		assertEquals("0x", ((SampleEntityClass) rs4iterator.next()).getData());
	}

}
