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
		jessy = LocalJessy.getInstance();

		// First, we have to define the entities read or written inside the
		// transaction
		jessy.addEntity(SampleEntityClass.class);
		jessy.addEntity(Sample2EntityClass.class);

		jessy.write(new SampleEntityClass("1", "sample entity"));
		jessy.write(new Sample2EntityClass("1", "sample 2 entity"));
	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.Jessy#nonTransactionalRead(java.lang.Class, java.lang.String)}
	 * .
	 */
	@Test
	public void testNonTransactionalRead() {
		try {
			SampleEntityClass sampleEntity = jessy.read(
					SampleEntityClass.class, "1");
			Sample2EntityClass sample2Entity = jessy.read(
					Sample2EntityClass.class, "1");
			assertEquals("Result", "1", sampleEntity.getSecondaryKey());
			assertEquals("Result", "1", sample2Entity.getSecondaryKey());

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Test method for
	 * {@link fr.inria.jessy.Jessy#nonTransactionalWrite(fr.inria.jessy.store.JessyEntity)}
	 * .
	 */
	@Test
	public void testNonTransactionalWrite() {
		try {
			jessy.write(new SampleEntityClass("2", "sample entity"));
			SampleEntityClass sampleEntity = jessy.read(
					SampleEntityClass.class, "2");
			assertEquals("Result", "2", sampleEntity.getSecondaryKey());
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
