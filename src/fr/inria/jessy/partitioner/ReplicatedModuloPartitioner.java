package fr.inria.jessy.partitioner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;



/**
 * 
 * ReplicatedModuloPartitioner implements a Partitioner that manage replication factors greater than 1 (i.e. entities can be replicated in more than one Jessy 
 * insance)
 * 
 * @author pcincilla
 *
 */
public class ReplicatedModuloPartitioner implements Partitioner{

	private int replicationFactor;
//	private int jessyInstances;
	private int roundIndex;

	public ReplicatedModuloPartitioner(){
		
		replicationFactor=ConstantPool.REPLICATION_FACTOR;
//		jessyInstances= JessyGroupManager.getInstance().getReplicaGroups().size();
		roundIndex=-1;
		
		//		ReplicatedModuloPartitioner assume that the group size is equal to 1, i.e. each group contains exactly one Jessy instance
		assert ConstantPool.GROUP_SIZE==1;

//		//		number of jessy instances can't be less than the replicationFactor
//		assert JessyGroupManager.getInstance().getReplicaGroups().size()>replicationFactor;
//
//		//		TODO handle any number of jessy instances
//		//		for simplicity for now assume that jessy instances are divisible by the replication factor
//		assert JessyGroupManager.getInstance().getReplicaGroups().size() % replicationFactor == 0;

	}

	/**
	 * return true if at least one group replicating key is local, false otherwise
	 */
	@Override
	public boolean isLocal(String k) {

		Collection<Group> tmp = JessyGroupManager.getInstance().getMyGroups();
		tmp.retainAll(resolveGroups(k));

		if (tmp.isEmpty()){
			return false;
		}
		return true;
	}

	@Override
	public <E extends JessyEntity> Set<Group> resolve(ReadRequest<E> readRequest) {

		List<Group> tmp = new ArrayList<Group>();
		Set<Group> ret= new HashSet<Group>();

		if (readRequest.isOneKeyRequest()) {
			tmp=resolveGroups(readRequest.getOneKey().getKeyValue().toString());
			ret.add(tmp.get(getNextRoundIndex()));
		} else {
			return null;
		}
		return ret;
	}

	@Override
	public Set<String> resolveNames(Set<String> keys) {
		Set<String> ret = new HashSet<String>();

		if (keys.size() == 0){
			return ret;
		}

		for (String k: keys){
			ret.addAll(resolveNames(k));
		}

		return ret;
	}


	/**
	 * given an Entity key returns all groups that replicate this Entity
	 * 
	 * @param key the key of the entity 
	 * @return a sorted set of groups the entity key belongs
	 */

	private List<Group> resolveGroups(String key) {

		//		TODO verify HashSet is the best structure
		List<Group> groupSet= new ArrayList<Group>();

		int numericKey = 0;
		String mkey = key.replaceAll( "[^\\d]", "" );
		if(!mkey.equals("")){
			numericKey = Integer.valueOf(mkey); 
		}

		//		the groupSetNumber
		int groupSetNumber=numericKey % replicationFactor;
		//		the first group of this group set
		int groupSetStart=groupSetNumber*replicationFactor;

		for(int i=0; i<replicationFactor; i++){
			groupSet.add(JessyGroupManager
					.getInstance()
					.getReplicaGroups()
					.get(groupSetStart+i));
		}
		return groupSet;
	}

	/**
	 * given an Entity key returns all names of groups that replicate this Entity
	 * 
	 * @param key the key of the entity 
	 * @return the set of groups that replicate he entity
	 */

	private Set<String> resolveNames(String key) {

		//		TODO verify HashSet is the best structure
		Set<String> groupSet= new HashSet<String>();

		int numericKey = 0;
		String mkey = key.replaceAll( "[^\\d]", "" );
		if(!mkey.equals("")){
			numericKey = Integer.valueOf(mkey); 
		}

		//		the groupSetNumber
		int groupSetNumber=numericKey % replicationFactor;
		//		the first group of this group set
		int groupSetStart=groupSetNumber*replicationFactor;

		for(int i=0; i<replicationFactor; i++){
			groupSet.add(JessyGroupManager
					.getInstance()
					.getReplicaGroups()
					.get(groupSetStart+i).name());
		}
		return groupSet;
	}

	/**
	 * Implements a round robin algorithm over replicas in group sets
	 * 
	 * @return the next index
	 */
	private int getNextRoundIndex() {
		roundIndex=roundIndex+1;
		roundIndex=roundIndex%replicationFactor;

		return roundIndex;
	}

}
