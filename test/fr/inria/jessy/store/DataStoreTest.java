package fr.inria.jessy.store;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.EntClass;

/**
 * @author Masoud Saeida Ardekani
 *
 */
public class DataStoreTest {

	DataStore dsPut;
	
	DataStore dsGet;
		
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
		String executionPath = System.getProperty("user.dir");
		dsPut = new DataStore(new File(executionPath), false,
		"myStore");
		dsPut.addPrimaryIndex("myStore", TestEntityClass.class);
		dsPut.addSecondaryIndex("myStore", TestEntityClass.class, Integer.class,
		"entityID");
		
		dsGet = new DataStore(new File(executionPath), false,
		"GetStore");
		dsGet.addPrimaryIndex("GetStore", TestEntityClass.class);
		dsGet.addSecondaryIndex("GetStore", TestEntityClass.class, Integer.class,
		"entityID");
		
		TestEntityClass ec;
		for (int i=0;i< 10000; i++ )
		{
			ec = new TestEntityClass( i,"ver1");
			dsGet.put(ec);
		}

	}

	/**
	 * Test method for {@link fr.inria.jessy.store.DataStore#put(fr.inria.jessy.store.JessyEntity)}.
	 */
	@Test
	public void testPut() {
		TestEntityClass ec = new TestEntityClass(1,"ver1");
		dsPut.put(ec);

		ec = new TestEntityClass(1,"ver2");
		dsPut.put(ec);
		
		assertEquals("Result", 2, dsPut.getEntityCounts(TestEntityClass.class, "entityID", 1));
		
		ec = new TestEntityClass(2,"ver1");
		dsPut.put(ec);
		
		assertEquals("Result", 1, dsPut.getEntityCounts(TestEntityClass.class, "entityID", 2));
	}
	
	/**
	 * Test method for {@link fr.inria.jessy.store.DataStore#get(java.lang.Class, java.lang.String, java.lang.Object, java.util.ArrayList)}.
	 */
	@Test
	public void testGet() {
		//TODO incorporate vectors in the test 
		TestEntityClass ec = new TestEntityClass(1,"ver1");

		TestEntityClass result=dsGet.get(TestEntityClass.class, "entityID", 1, null);
		
		assertEquals("Result",(Integer) 1, result.getEntityID());
	}

	/**
	 * Test method for {@link fr.inria.jessy.store.DataStore#put(fr.inria.jessy.store.JessyEntity)}.
	 * 
	 */
	@Test(timeout=1000)
	public void testPutPerformance() {
		TestEntityClass ec;
		for (int i=0;i< 50000; i++ )
		{
			ec = new TestEntityClass( i,"ver1");
			dsPut.put(ec);
		}
		
		assertTrue(true); 
		
	}
	
	/**
	 * Test method for {@link fr.inria.jessy.store.DataStore#put(fr.inria.jessy.store.JessyEntity)}.
	 */
	@Test(timeout=1000)
	public void testGetPerformance() {
		Random rnd=new Random(System.currentTimeMillis());
		int id;
		
		TestEntityClass result;
		for (int i=0;i< 15000; i++ )
		{
			id=rnd.nextInt(10000);
			result=dsGet.get(TestEntityClass.class, "entityID",  id, null);
		}
		
		assertTrue(true);
		
	}

}
