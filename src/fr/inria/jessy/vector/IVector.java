package fr.inria.jessy.vector;

import java.util.List;
import fr.inria.jessy.vector.ValueVector;;

public interface IVector<K> {

	public <V extends ValueVector<K, Integer> & IVector<K>> boolean isReadable(V other) throws NullPointerException;

	public <V extends ValueVector<K, Integer> & IVector<K>> boolean isListReadable(List<V> otherList)
			throws NullPointerException;

	public <V extends ValueVector<K, Integer> & IVector<K>> void update(List<V> readList, List<V> writeList);

	public Integer getValue(K value);

	public Integer getSelfValue();

	public K getSelfKey();

}
