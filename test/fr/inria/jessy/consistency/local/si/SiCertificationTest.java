package fr.inria.jessy.consistency.local.si;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.JessyFactory;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.consistency.local.si.transaction.InitTransaction;
import fr.inria.jessy.consistency.local.si.transaction.T1x;
import fr.inria.jessy.consistency.local.si.transaction.T2x;
import fr.inria.jessy.consistency.local.si.transaction.T3x;
import fr.inria.jessy.consistency.local.si.transaction.T4y;
import fr.inria.jessy.consistency.local.si.transaction.T5x;
import fr.inria.jessy.consistency.local.si.transaction.T6x;
import fr.inria.jessy.consistency.local.si.transaction.T7x;
import fr.inria.jessy.consistency.local.si.transaction.T8y;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.vector.ScalarVector;


/**
 * Test for check SI certification. Scenario: there are four pairs of transactions (t1x, t2x; t3x, t4y; t5x, t6x, t7x, t8y). Each pair 
 * of transactions is run concurrently. Pairs 1 and 2 are composed of nested transactions, pairs 3 and 4 of overlapping transactions. 
 * Pairs 1 and 3 are composed by conflicting transactions in such a way that transactions t1x and t6x has to abort. All others transactions 
 * are expected to commit.
 * 
 * * @author pcincilla
 *
 */

public class SiCertificationTest {
	
	
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
		jessy = JessyFactory.getLocalJessy();

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
		assertEquals("tr.seq.number", 0, ScalarVector.lastCommittedTransactionSeqNumber.get());
		result0=null;
		

//		first pair
		Future<ExecutionHistory> future1;
		future1 = pool.submit(new T1x(jessy));
		
		Future<ExecutionHistory> future2;
		future2 = pool.submit(new T2x(jessy));
		
		ExecutionHistory result1 = future1.get();
		assertEquals("Result", TransactionState.ABORTED_BY_CERTIFICATION, result1.getTransactionState());
		assertEquals("Value", "1x", ((SampleEntityClass)result1.getReadSet().getEntities().iterator().next()).getData());

		ExecutionHistory result2 = future2.get();
		assertEquals("Result", TransactionState.COMMITTED, result2.getTransactionState());
		assertEquals("Value", "2x", ((SampleEntityClass)result2.getReadSet().getEntities().iterator().next()).getData());
		assertEquals("tr.seq.number", 1, ScalarVector.lastCommittedTransactionSeqNumber.get());
		
		result1=null;
		result2=null;
		
		
//		second pair
		Future<ExecutionHistory> future3;
		future3 = pool.submit(new T3x(jessy));
		assertEquals("tr.seq.number", 1, ScalarVector.lastCommittedTransactionSeqNumber.get());
		
		Future<ExecutionHistory> future4;
		future4 = pool.submit(new T4y(jessy));
		assertEquals("tr.seq.number", 1, ScalarVector.lastCommittedTransactionSeqNumber.get());
		
		ExecutionHistory result3 = future3.get();
		ExecutionHistory result4 = future4.get();
		
		
		assertEquals("Result", TransactionState.COMMITTED, result3.getTransactionState());
		assertEquals("Value", "3x", ((SampleEntityClass)result3.getReadSet().getEntities().iterator().next()).getData());
		assertEquals("Result", TransactionState.COMMITTED, result4.getTransactionState());
		assertEquals("Value", "4y", ((Sample2EntityClass)result4.getReadSet().getEntities().iterator().next()).getData());
		assertEquals("tr.seq.number", 3, ScalarVector.lastCommittedTransactionSeqNumber.get());
				
		result3=null;
		result4=null;
		
		
//		third pair
		Future<ExecutionHistory> future5;
		future5 = pool.submit(new T5x(jessy));
		
		Future<ExecutionHistory> future6;
		future6 = pool.submit(new T6x(jessy));
		
		ExecutionHistory result5 = future5.get();
		assertEquals("Result", TransactionState.COMMITTED, result5.getTransactionState());
		assertEquals("Value", "5x",  ((SampleEntityClass)result5.getReadSet().getEntities().iterator().next()).getData());
		assertEquals("tr.seq.number", 4, ScalarVector.lastCommittedTransactionSeqNumber.get());
		
		ExecutionHistory result6 = future6.get();
		assertEquals("Result", TransactionState.ABORTED_BY_CERTIFICATION, result6.getTransactionState());
		assertEquals("Value", "6x",  ((SampleEntityClass)result6.getReadSet().getEntities().iterator().next()).getData());
		assertEquals("tr.seq.number", 4, ScalarVector.lastCommittedTransactionSeqNumber.get());
		
		result5=null;
		result6=null;
		
		
//		fourth pair
		Future<ExecutionHistory> future7;
		future7 = pool.submit(new T7x(jessy));
		
		Future<ExecutionHistory> future8;
		future8 = pool.submit(new T8y(jessy));
		
		ExecutionHistory result7 = future7.get();
		assertEquals("Result", TransactionState.COMMITTED, result7.getTransactionState());
		assertEquals("Value", "7x",  ((SampleEntityClass)result7.getReadSet().getEntities().iterator().next()).getData());
		assertEquals("tr.seq.number", 5, ScalarVector.lastCommittedTransactionSeqNumber.get());
		
		ExecutionHistory result8 = future8.get();
		assertEquals("Result", TransactionState.COMMITTED, result8.getTransactionState());
		assertEquals("Value", "8y",  ((Sample2EntityClass)result8.getReadSet().getEntities().iterator().next()).getData());
		assertEquals("tr.seq.number", 6, ScalarVector.lastCommittedTransactionSeqNumber.get());
	}

	
	
}
