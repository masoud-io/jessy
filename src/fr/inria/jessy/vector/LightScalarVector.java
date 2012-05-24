package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.sleepycat.persist.model.Persistent;

/**
 * This is a simple ScalarVector implementation. It will be used for
 * implementing p-store like data store. I.e., during execution, read the most
 * recent committed versions. During termination, check to see if the already
 * read values have been modified or not.
 * <p>
 * The main difference between this class and {@code ScalarVector} is that this
 * implementation is so light. It does not consider reading consistent snapshots
 * during reads. Thus, it may end up taking an inconsistent snapshot from the
 * database.
 * 
 * 
 * @author Masoud Saeida Ardekani
 * 
 * @param <K>
 */
@Persistent
public class LightScalarVector<K> extends Vector<K> implements Externalizable {

	int version = 0;

	/**
	 * Needed for BerkeleyDB
	 */
	@Deprecated
	public LightScalarVector() {
	}

	@Override
	public boolean isCompatible(Vector<K> other) throws NullPointerException {
		if (version == ((LightScalarVector<K>) other).version)
			return true;
		else
			return false;

	}

	/**
	 * This method always returns true. This leads to reading the very last
	 * committed entity.
	 */
	@Override
	public boolean isCompatible(CompactVector<K> other)
			throws NullPointerException {
		return true;
	}

	@Override
	public void update(CompactVector<K> readSet, CompactVector<K> writeSet) {
		version++;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		version = in.readInt();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(version);
	}

}
