package fr.inria.jessy.store;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.entity.SampleEntityClass;
import fr.inria.jessy.utils.Compress;

/**
 * @author Masoud Saeida Ardekani
 * 
 */
public class DataStoreTest extends TestCase {

	DataStore dataStore;

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
		dataStore = new DataStoreFactory().getDataStoreInstance();
		dataStore.addPrimaryIndex( SampleEntityClass.class);
		dataStore.addSecondaryIndex( SampleEntityClass.class,
				String.class, "secondaryKey");

		
		//fill datastore
		SampleEntityClass ec = new SampleEntityClass("1", "ver1");
		dataStore.put(ec);

		ec = new SampleEntityClass("1", "ver2");
		dataStore.put(ec);

	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.store.DataStore#put(fr.inria.jessy.store.JessyEntity)}
	 * .
	 */
	@Test
	public void testPut() {

		assertEquals("Result", 2, dataStore.getEntityCounts(
				Compress.compressClassName(SampleEntityClass.class.getName()), "secondaryKey", "1"));

		SampleEntityClass ec = new SampleEntityClass("2", "ver1");
		dataStore.put(ec);

		assertEquals("Result", 1, dataStore.getEntityCounts(
				Compress.compressClassName(SampleEntityClass.class.getName()), "secondaryKey", "2"));
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
		ReadReply<SampleEntityClass> reply = dataStore.get(readRequest);
		assertEquals("Result", "ver2", reply.getEntity().iterator().next().getData());
	}


}
