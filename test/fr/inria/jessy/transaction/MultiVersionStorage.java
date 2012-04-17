package fr.inria.jessy.transaction;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.entity.SampleEntityClass;

/**
 * this class test storage (read/write) of multi-version objects inside jessy in a transactional way.
 * TODO UNDONE!!!
 * @author Masoud Saeida Ardekani
 * 
 */

public class MultiVersionStorage {

	Jessy jessy;
	
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
		jessy.addEntity(SampleEntityClass.class);
		
	}
	
	/**
	 * Test method for
	 * .
	 */
	@Test
	public void testMultiVersionStorage() {
		try {
			Transaction createFirstVersion=new Transaction(jessy) {
				
				@Override
				public ExecutionHistory execute() {
					SampleEntityClass sampleEntity =new SampleEntityClass("1", "ver1");
					write(sampleEntity);
					return commitTransaction();
				}
			};
			

			Transaction createSecondVersion=new Transaction(jessy) {
				
				@Override
				public ExecutionHistory execute() {
					
					SampleEntityClass sampleEntity;
					try {
						sampleEntity = read(SampleEntityClass.class, "1");
						sampleEntity.setData("ver2");
						write(sampleEntity);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return commitTransaction();
				}
			};


			Transaction readTrans=new Transaction(jessy) {
				
				@Override
				public ExecutionHistory execute() {
					
					SampleEntityClass sampleEntity;
					try {
						sampleEntity = read(SampleEntityClass.class, "1");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return commitTransaction();
				}
			};


			createFirstVersion.execute();
			createSecondVersion.execute();
			readTrans.execute();
			
			assertTrue(true);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
