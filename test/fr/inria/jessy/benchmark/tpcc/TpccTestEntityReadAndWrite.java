package fr.inria.jessy.benchmark.tpcc;

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
import fr.inria.jessy.benchmark.tpcc.entities.District;
import fr.inria.jessy.consistency.local.si.transaction.InitTransaction;
import fr.inria.jessy.entity.Sample2EntityClass;
import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionState;

public class TpccTestEntityReadAndWrite extends TestCase{

	Jessy jessy;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() {
		PropertyConfigurator.configure("log4j.properties");

		try {
			jessy = DistributedJessy.getInstance();

			jessy.addEntity(District.class);
			jessy.addEntity(SampleEntityClass.class);
			jessy.addEntity(Sample2EntityClass.class);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test() {
		try{
			
			
			/*	TEST SampleEntityClass*/
			ExecutorService pool = Executors.newFixedThreadPool(3);
			
			Future<ExecutionHistory> futureInit;
			futureInit = pool.submit(new InitTransaction(jessy));
			
			ExecutionHistory init=futureInit.get();
			assertEquals("Result", TransactionState.COMMITTED, init.getTransactionState());
			
			Future<ExecutionHistory> future1;
			future1 = pool.submit(new ReadAndWriteOnSampleEntity(jessy));
			
			Future<ExecutionHistory> future2;
			future2 = pool.submit(new ReadAndWriteOnSampleEntity(jessy));
			
			
			ExecutionHistory result1 = future1.get();
			assertEquals("Value", "0x", ((SampleEntityClass)result1.getReadSet().getEntities().iterator().next()).getData());
			assertEquals("Result", TransactionState.COMMITTED, result1.getTransactionState());
			
			ExecutionHistory result2 = future2.get();
			assertEquals("Value", "0x", ((SampleEntityClass)result2.getReadSet().getEntities().iterator().next()).getData());
			assertEquals("Result", TransactionState.COMMITTED, result2.getTransactionState());
			
			
			
			/*	TEST District*/
			District d;
			
//			read and write the district entity with warehouse id=1 and district id=1
			System.out.println("Testing District entity");
			System.out.println("first read and write on District");
			d=readAndWriteDistrict(1, 1);
			System.out.println();
			
			if(d.getD_ID()==null){
				System.err.println("Error: the District id is null");
				assertFalse(true);
			}

			Thread.sleep(1000);
			
//			read and write the district entity with warehouse id=1 and district id=1
			System.out.println();
			System.out.println("second read and write on District");
			d=readAndWriteDistrict(1, 1);
			System.out.println();

			if(d.getD_ID()==null){
				System.err.println("Error: the District id is null");
				assertFalse(true);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			assert false;
		}
	}

	private District readAndWriteDistrict(int warehouseId, int districtId) throws Exception {

		ReandAndWriteOnDistrict rwd = new ReandAndWriteOnDistrict(jessy, warehouseId, districtId);
		return rwd.readAndWrite();
	}
}
