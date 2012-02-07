package fr.inria.jessy.store;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.EntClass;

/**
 * @author Masoud Saeida Ardekani
 *
 */
public class DataStoreTest {

	DataStore ds;
		
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
		ds = new DataStore(new File(executionPath), false,
		"myStore");
		ds.addPrimaryIndex("myStore", TestEntityClass.class);
		ds.addSecondaryIndex("myStore", TestEntityClass.class, String.class,
		"entityID");
	}

	/**
	 * Test method for {@link fr.inria.jessy.store.DataStore#put(fr.inria.jessy.store.JessyEntity)}.
	 */
	@Test
	public void testPut() {
		TestEntityClass ec = new TestEntityClass();
		ec.setEntityID("1");
		ec.setData("ver1");
		ds.put(ec);

		ec = new TestEntityClass();
		ec.setEntityID("1");
		ec.setData("ver2");
		ds.put(ec);
		
		assertEquals("Result", 2, ds.getEntityCounts(TestEntityClass.class, "entityID", "1"));
		
		ec = new TestEntityClass();
		ec.setEntityID("2");
		ec.setData("ver1");
		ds.put(ec);
		
		assertEquals("Result", 1, ds.getEntityCounts(TestEntityClass.class, "entityID", "2"));
	}

	/**
	 * Test method for {@link fr.inria.jessy.store.DataStore#get(java.lang.Class, java.lang.String, java.lang.Object, java.util.ArrayList)}.
	 */
	@Test
	public void testGet() {
		//TODO incorporate vectors in the test 
		TestEntityClass ec = new TestEntityClass();
		ec.setEntityID("1");
		ec.setData("ver1");
		ds.put(ec);

		ec = new TestEntityClass();
		ec.setEntityID("1");
		ec.setData("ver2");
		ds.put(ec);

		TestEntityClass result=ds.get(TestEntityClass.class, "entityID", "1", null);
		
		assertEquals("Result", "1" , result.getEntityID());
	}

}
