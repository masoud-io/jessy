package fr.inria.jessy.store;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.entity.SampleEntityClass;

/**
 * @author Masoud Saeida Ardekani
 * 
 */
public class DataStoreTest extends TestCase {

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
			ec = new SampleEntityClass("" + i, "ver1_of" + i);
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

		ReadRequest<SampleEntityClass> readRequest = new ReadRequest<SampleEntityClass>(
				SampleEntityClass.class, "secondaryKey", "1", null);
		ReadReply<SampleEntityClass> reply = dsGet2.get(readRequest);

		assertEquals("Result", "ver1_of1", reply.getEntity().iterator().next().getData());
	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.store.DataStore#delete(Class, String, Object)}.
	 */
	@Test
	public void testDelete() {
		ReadRequest<SampleEntityClass> readRequest = new ReadRequest<SampleEntityClass>(
				SampleEntityClass.class, "secondaryKey", "0", null);
		ReadReply<SampleEntityClass> reply = dsGet.get(readRequest);
		assertEquals("Result", "0", reply.getEntity().iterator().next().getKey());

		boolean deleteResult = dsGet.delete(SampleEntityClass.class,
				"secondaryKey", "" + 0);

		assertFalse(!deleteResult);

		ReadReply<SampleEntityClass> reply2;
		reply2 = dsGet.get(readRequest);
		assertTrue(reply.getEntity().iterator().hasNext());

	}

}
