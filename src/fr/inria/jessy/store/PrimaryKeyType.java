package fr.inria.jessy.store;

import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;

@Persistent
public class PrimaryKeyType implements Comparable<PrimaryKeyType> {
	@KeyField(1)
	public Long primaryKey;

	@Override
	public int compareTo(PrimaryKeyType o) {
		return -1 * primaryKey.compareTo(o.primaryKey);
	}

}