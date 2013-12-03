package fr.inria.jessy.partitioner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.utils.Configuration;

/**
 * This class implements a simple sequential partitioner as follows:
 * <p>
 * Attention: This class implementation may not be safe regarding some keys.
 * Attention: This class does not support dynamic groups.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
// TODO InComplete!!!
public class ReplicatedSequentialPartitioner extends Partitioner {

	private static Logger logger = Logger
			.getLogger(ReplicatedSequentialPartitioner.class);

	private static int totalNumberOfObjects;
	
	private static int numberOfObjectsPerGroup=-1;
	
	private static int replicationFactor;

	static {
		Properties myProps = new Properties();
		FileInputStream MyInputStream;
		try {
			MyInputStream = new FileInputStream(
					fr.inria.jessy.ConstantPool.NUMBER_OF_OBJECTS_PROPERTY_FILE);
			myProps.load(MyInputStream);
			totalNumberOfObjects = Integer.parseInt(myProps
					.getProperty(ConstantPool.NUMBER_OF_OBJECTS_PROPERTY_NAME));
			
			replicationFactor=Integer.parseInt(Configuration.readConfig(fr.inria.jessy.ConstantPool.REPLICATION_FACTOR));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException ee) {
			ee.printStackTrace();
		}catch (Exception ex){
			ex.printStackTrace();
		}
	}

	public ReplicatedSequentialPartitioner(JessyGroupManager m) {
		super(m);

	}

	@Override
	public <E extends JessyEntity> Set<Group> resolve(ReadRequest<E> readRequest) {
		Set<Group> ret = new HashSet<Group>();

		if (readRequest.isOneKeyRequest()) {
			ret.add(randomResolve(readRequest.getOneKey().getKeyValue().toString()));
		} else {
			// TODO
			return null;
		}

		return ret;
	}

	@Override
	public boolean  isLocal(String k) {
		List<Group> groups=resolveAll(k);

		for(Group g : groups){
			if (manager.getMyGroups().contains(g))
				return true;
		}
		return false;
	}

	@Override
	public Set<String> resolveNames(Set<String> keys) {
		Set<String> result = new HashSet<String>();

		/*
		 * return if there is no key!
		 */
		if (keys.size() == 0)
			return result;

		List<Group> groups;
		for (String key : keys) {
			groups=resolveAll(key);
			for (Group g : groups)
				result.add(g.name());
		}
		logger.debug("keys " + keys + " are resolved to" + result);
		return result;
	}

	public Group resolve(String key) {
		return resolveAll(key).get(0);
	}

	private Group randomResolve(String key) {
		List<Group> groups=resolveAll(key);
		Random randomGenerator = new Random();
		int index = randomGenerator.nextInt(groups.size());		 
		return groups.get(index);
	}
	
	/**
	 * This methods returns the group of a key.
	 * 
	 * @param k
	 *            a key
	 * @return the replica group of <i>k</i>.
	 */
	@Override	
	public List<Group> resolveAll(String key) {
		int numericKey = 0;
		String mkey = key.replaceAll("[^\\d]", "");
		if (!mkey.equals("")) {
			numericKey = Integer.valueOf(mkey);
		}
		
		if (numberOfObjectsPerGroup==-1)
			numberOfObjectsPerGroup=totalNumberOfObjects/(manager.getReplicaGroups().size()/replicationFactor);
		
		List<Group> results=new ArrayList<Group>();
		
		for(int i=0;i< manager.getReplicaGroups()
				.size();i++){
			int j= i % (manager.getReplicaGroups().size()/replicationFactor);
			if ((numericKey<((j+1)*numberOfObjectsPerGroup)) && 
					(numericKey>=((j)*numberOfObjectsPerGroup))){
				results.add(manager
						.getReplicaGroups()
						.get(i));
			}
		}
		return results;

	}

	@Override
	public Set<String> generateKeysInAllGroups() {
		Set<String> keys=new HashSet<String>();
		
		for (int i = 0; i < manager.getReplicaGroups().size(); i++) {
			int key=(i*numberOfObjectsPerGroup)+1;
			keys.add(""+key);
		}
		return keys;
	}
	
	@Override
	public Set<String> generateLocalKey() {
		Set<String> keys=new HashSet<String>();
		
		for (int i = 0; i < manager.getReplicaGroups().size(); i++) {
			int key=(i*numberOfObjectsPerGroup)+1;
			if (isLocal(""+key)){				
				keys.add(""+key);
				return keys;
			}
		}
		return keys;
	}

}
