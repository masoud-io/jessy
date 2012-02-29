/**
 * 
 */
package fr.inria.jessy.consistency.nmsi;

import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.transaction.SampleTransaction;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author Masoud Saeida Ardekani
 *
 */
public class LocalJessy_NMSI {

	LocalJessy jessy;
	SampleTransaction st;
	
	public static Test suite() {
		TestSuite suite = new TestSuite(LocalJessy_NMSI.class.getName());
		//$JUnit-BEGIN$

		//$JUnit-END$
		return suite;
	}

}
