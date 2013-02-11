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
import fr.inria.jessy.utils.Configuration;



/**
 * 
 * ReplicatedModuloPartitioner implements a Partitioner that manage replication factors greater (i.e. how many jessy instances 
 * replicate each entity. It suppose (i)GROUP_SIZE==1 (ii) jessy instances are divisible by the replication factor i.e. all groups have 
 * the same number of jessy instances.)
 * 
 * @author pcincilla
 *
 */
public class ReplicatedModuloPartitioner implements Partitioner{

	private int replicationFactor = -1;
	private int groupMembersNumber=-1;
	private int roundIndex;
	
	public ReplicatedModuloPartitioner(){
		
		roundIndex=-1;

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

		if(replicationFactor==-1||groupMembersNumber==-1){
			setUpVariables();
		}

		//		the groupSetNumber
		int groupSetNumber=numericKey % (replicationFactor-1);
		//		the first group of this group set
		int groupSetStart=groupSetNumber*(replicationFactor-1);

		for(int i=0; i<groupMembersNumber; i++){
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

		if(replicationFactor==-1||groupMembersNumber==-1){
			setUpVariables();
		}
		
		//		the groupSetNumber
		int groupSetNumber=numericKey % (replicationFactor-1);
		//		the first group of this group set
		int groupSetStart=groupSetNumber*(replicationFactor-1);

		for(int i=0; i<groupMembersNumber; i++){
			
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
		roundIndex=roundIndex%(groupMembersNumber);

		return roundIndex;
	}
	
	private void setUpVariables(){
		
		
		replicationFactor=Integer.parseInt(Configuration.readConfig(fr.inria.jessy.ConstantPool.REPLICATION_FACTOR));
		groupMembersNumber=JessyGroupManager.getInstance().getReplicaGroups().size()/(replicationFactor);
		
	
		//		ReplicatedModuloPartitioner assume that the group size is equal to 1, i.e. each group contains exactly one Jessy instance
		if( JessyGroupManager.getInstance().getGroupSize()!=1){
			System.err.println("ReplicatedModuloPartitioner is used with GROUP_SIZE!=1, system will exit");
			System.exit(1);
		}	

		//		TODO handle any number of jessy instances
		//		for simplicity for now assume that jessy instances are divisible by the replication factor
		if( !(JessyGroupManager.getInstance().getReplicaGroups().size() % (replicationFactor) == 0)){
			System.err.println("ReplicatedModuloPartitioner is used and jessy instances are NOT divisible by the replication factor, system will exit");
			System.exit(1);
		}
		
	}

	@Override
	public Set<String> generateKeysInAllGroups() {
		//TODO
		return null;
	}

}
