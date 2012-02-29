package fr.inria.jessy.store;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.vector.DependenceVector;
import fr.inria.jessy.vector.Vector;

/**
 * @author Masoud Saeida Ardekani
 * 
 */
public class DataStoreTest {

	DataStore dsPut;

	DataStore dsGet;
	DataStore dsGet2;

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
		dsPut = new DataStore(new File(executionPath), false, "myStore");
		dsPut.addPrimaryIndex("myStore", SampleEntityClass.class);
		dsPut.addSecondaryIndex("myStore", SampleEntityClass.class,
				String.class, "secondaryKey");

		dsGet = new DataStore(new File(executionPath), false, "GetStore");
		dsGet.addPrimaryIndex("GetStore", SampleEntityClass.class);
		dsGet.addSecondaryIndex("GetStore", SampleEntityClass.class,
				String.class, "secondaryKey");

		dsGet2 = new DataStore(new File(executionPath), false, "GetStore");
		dsGet2.addPrimaryIndex("GetStore", SampleEntityClass.class);
		dsGet2.addSecondaryIndex("GetStore", SampleEntityClass.class,
				String.class, "secondaryKey");

		SampleEntityClass ec;
		for (int i = 0; i < 10000; i++) {
			ec = new SampleEntityClass("" + i, "ver1");
			dsGet.put(ec);
		}

	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.store.DataStore#put(fr.inria.jessy.store.JessyEntity)}
	 * .
	 */
	@Test
	public void testPut() {
		SampleEntityClass ec = new SampleEntityClass("1", "ver1");
		dsPut.put(ec);

		ec = new SampleEntityClass("1", "ver2");
		dsPut.put(ec);

		assertEquals("Result", 2, dsPut.getEntityCounts(
				SampleEntityClass.class, "secondaryKey", "1"));

		ec = new SampleEntityClass("2", "ver1");
		dsPut.put(ec);

		assertEquals("Result", 1, dsPut.getEntityCounts(
				SampleEntityClass.class, "secondaryKey", "2"));
	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.store.DataStore#get(java.lang.Class, java.lang.String, java.lang.Object, java.util.ArrayList)}
	 * .
	 */
	@Test
	public void testGet() {
		// TODO incorporate vectors in the test

		SampleEntityClass result = dsGet2.get(SampleEntityClass.class,
				"secondaryKey", "1");

		assertEquals("Result", "1", result.getSecondaryKey());
	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.store.DataStore#delete(Class, String, Object)}.
	 */
	@Test
	public void testDelete() {
		SampleEntityClass getResult = dsGet.get(SampleEntityClass.class,
				"secondaryKey", "0");
		assertEquals("Result", "0", getResult.getSecondaryKey());

		boolean deleteResult = dsGet.delete(SampleEntityClass.class,
				"secondaryKey", "" + 0);

		assertFalse(!deleteResult);

		getResult = dsGet.get(SampleEntityClass.class, "secondaryKey", "0");
		assertNull(getResult);

	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.store.DataStore#put(fr.inria.jessy.store.JessyEntity)}
	 * .
	 * 
	 */
	@Test(timeout = 1000)
	public void testPutPerformance() {
		SampleEntityClass ec;
		for (int i = 0; i < 50000; i++) {
			ec = new SampleEntityClass("" + i, "ver1");
			dsPut.put(ec);
		}

		assertTrue(true);

	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.store.DataStore#put(fr.inria.jessy.store.JessyEntity)}
	 * .
	 */
	@Test(timeout = 1000)
	public void testGetPerformance() {
		Random rnd = new Random(System.currentTimeMillis());
		int id;

		SampleEntityClass result;
		for (int i = 0; i < 10000; i++) {
			id = rnd.nextInt(10000);
			result = dsGet2.get(SampleEntityClass.class, "secondaryKey", ""
					+ id);
		}

		assertTrue(true);

	}

}
