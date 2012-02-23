/**
 * 
 */
package fr.inria.jessy;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;

/**
 * @author msaeida
 *
 */
public class JessyTest {
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
		jessy=LocalJessy.getInstance();
		
		// First, we have to define the entities read or written inside the transaction
		jessy.addEntity(SampleEntityClass.class);
		jessy.addEntity(Sample2EntityClass.class);
		
		jessy.nonTransactionalWrite(new SampleEntityClass("1", "sample entity"));
		jessy.nonTransactionalWrite(new Sample2EntityClass("1", "sample 2 entity"));
	}

	/**
	 * Test method for {@link fr.inria.jessy.Jessy#nonTransactionalRead(java.lang.Class, java.lang.String)}.
	 */
	@Test
	public void testNonTransactionalRead() {
		SampleEntityClass sampleEntity=jessy.nonTransactionalRead(SampleEntityClass.class, "1");
		Sample2EntityClass sample2Entity=jessy.nonTransactionalRead(Sample2EntityClass.class, "1");
		assertEquals("Result", "1", sampleEntity.getSecondaryKey());
		assertEquals("Result", "1", sample2Entity.getSecondaryKey());
	}

	/**
	 * Test method for {@link fr.inria.jessy.Jessy#nonTransactionalWrite(fr.inria.jessy.store.JessyEntity)}.
	 */
	@Test
	public void testNonTransactionalWrite() {
		jessy.nonTransactionalWrite(new SampleEntityClass("2", "sample entity"));
		SampleEntityClass sampleEntity=jessy.nonTransactionalRead(SampleEntityClass.class, "2");
		assertEquals("Result", "2", sampleEntity.getSecondaryKey());		
	}

}
