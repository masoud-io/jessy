package fr;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

/**
 * @author Masoud Saeida Ardekani
 * 
 */
public class AccessEntClass {

	PrimaryIndex<Long, EntClass> pindex;
	SecondaryIndex<String, Long, EntClass> sindex;

	public AccessEntClass(EntityStore store) {
		pindex = store.getPrimaryIndex(Long.class, EntClass.class);

		sindex = store.getSecondaryIndex(pindex, String.class, "secondaryKey");

	}
}
