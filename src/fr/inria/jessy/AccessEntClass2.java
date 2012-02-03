package fr.inria.jessy;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

/**
 * @author Masoud Saeida Ardekani
 * 
 */
public class AccessEntClass2 {

	PrimaryIndex<String, EntClass2> pindex;
	SecondaryIndex<String, String, EntClass2> sindex;

	public AccessEntClass2(EntityStore store) {
		pindex = store.getPrimaryIndex(String.class, EntClass2.class);

		sindex = store.getSecondaryIndex(pindex, String.class, "sKey");

	}
}
