/**
 * 
 */
package fr.inria.jessy.consistency.nmsi;

import org.junit.Before;

import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.store.Sample2EntityClass;
import fr.inria.jessy.store.SampleEntityClass;
import fr.inria.jessy.transaction.SampleTransaction;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author Masoud Saeida Ardekani
 *
 */
public class LocalJessy_NMSI {

	LocalJessy jessy;
	SampleTransaction1 st1;
	SampleTransaction2 st2;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		jessy=LocalJessy.getInstance();

		// First, we have to define the entities read or written inside the transaction
		jessy.addEntity(SampleEntityClass.class);
		jessy.addEntity(Sample2EntityClass.class);
		
		st1=new SampleTransaction1(jessy);
		st2=new SampleTransaction2(jessy);
	}
	
	public static Test suite() {
		TestSuite suite = new TestSuite(LocalJessy_NMSI.class.getName());
		//$JUnit-BEGIN$

		//$JUnit-END$
		return suite;
	}

}
