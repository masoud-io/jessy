package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is a concurrent version of {@code VersionVector}. It is needed for
 * storing version vectors associated to each jessy instance in the PSI
 * consistency criterion.
 * <p>
 * {@code VersionVector} cannot be used for vectors associated to each jessy
 * instance since its implementation is not thread safe.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class ConcurrentVersionVector<K> implements Externalizable,
		Comparable<CompactVector<K>> {

	private static final Integer bydefault = -1;

	private ConcurrentHashMap<K, Integer> map;
	private K selfKey;

	public ConcurrentVersionVector(K selfKey) {
		map = new ConcurrentHashMap<K, Integer>();
		this.selfKey = selfKey;
	}

	public Integer getValue(K key) {

		if (map.keySet().contains(key))
			return map.get(key);
		else {
			return bydefault;
		}
	}

	public ConcurrentHashMap<K, Integer> getVector() {
		return map;
	}

	public void setVector(K k, Integer value) {
		map.put(k, value);
	}

	public K getSelfKey() {
		return selfKey;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		map = (ConcurrentHashMap) in.readObject();
		selfKey = (K) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(map);
		out.writeObject(selfKey);
	}

	@Override
	public int compareTo(CompactVector<K> o) {
		for (K k : o.getKeys()) {
			if (map.contains(k) && map.get(k).compareTo(o.getValue(k)) < 0)
				return -1;
		}

		return 1;
	}

	@Override
	public String toString() {
		String result = "";
		for (K k : map.keySet()) {
			result = result + " " + k + ":" + map.get(k);
		}
		return result;
	}

}
