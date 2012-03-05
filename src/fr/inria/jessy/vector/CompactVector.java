/**
 * 
 */
package fr.inria.jessy.vector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Masoud Saeida Ardekani
 * 
 *         This class implements a generic and serializable interface. It is
 *         used during read requests, and since it is compact, it is perfect for
 *         sending over the network
 */
public class CompactVector<K> extends ValueVector<K, Integer> implements
		Serializable {

	private static final long serialVersionUID = -6497349291347950406L;

	private List<K> keys;

	public CompactVector() {
		super(-1);
		keys = new ArrayList<K>();
	}

	public void addItem(K key, Integer value) {
		super.setValue(key, value);
		keys.add(key);
	}
}
