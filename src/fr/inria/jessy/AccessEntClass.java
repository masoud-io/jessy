package fr.inria.jessy;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

/**
 * @author Masoud Saeida Ardekani
 * 
 */
public class AccessEntClass {

	PrimaryIndex<String, EntClass> pindex;
	SecondaryIndex<String, String, EntClass> sindex;

	public AccessEntClass(EntityStore store) {
		pindex = store.getPrimaryIndex(String.class, EntClass.class);

		sindex = store.getSecondaryIndex(pindex, String.class, "sKey");

	}
}
