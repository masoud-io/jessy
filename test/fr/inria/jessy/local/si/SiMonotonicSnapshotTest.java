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
import fr.inria.jessy.local.si.transaction.*;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionState;


/**
 * 
 * Check the snapshot of transactions in a scenario in which nmsi will return a non-monotonic snapshot.
 * Scenario: There are four concurrent transactions T1, T2, T3 and T4. T1 and T4 start concurrently. T1 read x 
 * and T4 read y. After T1 and T4 has read T2 and T3 start and commit. T2 write on x and T3 write on y. When T2 and T3 
 * are committed transactions T1 and T4 reads y and x, respectively. In SI T1 and T4 must both read the initial value 
 * of x and y. In nmsi they read the last committed value.
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

//		INIT
		Future<ExecutionHistory> futureInit;
		futureInit = pool.submit(new InitTransaction(jessy));
		
		ExecutionHistory result0 = futureInit.get();
		assertEquals("Result", TransactionState.COMMITTED, result0.getTransactionState());
		assertEquals("tr.seq.number", 0, result0.getTransactionHandler().getTransactionSeqNumber());
		
//		assertEquals("committedTr.seq.number", 1, Jessy.lastCommittedTransactionSeqNumber.get());
		result0=null;
		futureInit=null;
		
		Future<ExecutionHistory> future1;
		Future<ExecutionHistory> future2;
		Future<ExecutionHistory> future3;
		Future<ExecutionHistory> future4;
		
		future1 = pool.submit(new T1(jessy));
		future4 = pool.submit(new T4(jessy));
		future2 = pool.submit(new T2(jessy));
		future3 = pool.submit(new T3(jessy));
	
	
		future2.get();
		future3.get();
		
		ExecutionHistory result1 = future1.get();
		ExecutionHistory result4 = future4.get();
		
		Iterator<JessyEntity> rs1iterator = result1.getReadSet().getEntities().iterator();
		Iterator<JessyEntity> rs4iterator = result4.getReadSet().getEntities().iterator();
		
		assertEquals("Value", "0y", ((Sample2EntityClass)rs1iterator.next()).getData());
		assertEquals("Value", "0x", ((SampleEntityClass)rs1iterator.next()).getData());

		assertEquals("Value", "0y", ((Sample2EntityClass)rs4iterator.next()).getData());
		assertEquals("Value", "0x", ((SampleEntityClass)rs4iterator.next()).getData());
	}
	
	
}
