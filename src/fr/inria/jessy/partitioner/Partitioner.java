package fr.inria.jessy.partitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

public abstract class Partitioner {

	JessyGroupManager manager;
	
	public Partitioner(JessyGroupManager m) {
		manager = m;
	}

	public abstract <E extends JessyEntity> Set<Group> resolve(
			ReadRequest<E> readRequest);

	
	/**
	 * Returns one group that is replicating the given key.
	 * If the key is replicated in several groups, only one of them deterministically sould be returned.
	 *  
	 * @param key
	 * @return
	 */
	public abstract Group resolve(String key);
	
	
	/**
	 * Returns the list of all groups replicating the given key.
	 * @param key
	 * @return
	 */
	public List<Group> resolveAll(String key) {
		List<Group> result=new ArrayList<Group>();
		result.add(resolve(key));
		return result;
	}
	
	public abstract boolean isLocal(String k);

	public abstract Set<String> resolveNames(Set<String> keys);
	
	public abstract Set<String> generateKeysInAllGroups();
}
