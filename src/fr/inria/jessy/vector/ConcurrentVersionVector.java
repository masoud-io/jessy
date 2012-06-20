package fr.inria.jessy.vector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.org.apache.bcel.internal.generic.Select;

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

	private ConcurrentHashMap<K, Integer> map;
	private K selfKey;

	public ConcurrentVersionVector(K selfKey) {
		map = new ConcurrentHashMap<K, Integer>();
		this.selfKey = selfKey;
	}

	public void update(Set<Entry<K, Integer>> vector) {
		if (vector == null)
			return;
		for (Entry<K, Integer> entry : vector) {
			K key = entry.getKey();
			Integer value = entry.getValue();
			try {
				if (map.get(key).compareTo(value) < 0) {
					map.put(key, value);
				}
			} catch (Exception ex) {
				map.put(key, value);
			}

		}

	}

	public Map<K, Integer> getMap() {
		return map;
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
}
