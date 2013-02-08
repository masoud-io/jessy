package fr.inria.jessy.partitioner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

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
public class SequentialPartitioner implements Partitioner {

	private static Logger logger = Logger
			.getLogger(SequentialPartitioner.class);

	private static int totalNumberOfObjects;
	
	private static int numberOfObjectsPerGroup=-1;

	static {
		Properties myProps = new Properties();
		FileInputStream MyInputStream;
		try {
			MyInputStream = new FileInputStream(
					fr.inria.jessy.ConstantPool.NUMBER_OF_OBJECTS_PROPERTY_FILE);
			myProps.load(MyInputStream);
			totalNumberOfObjects = Integer.parseInt(myProps
					.getProperty(ConstantPool.NUMBER_OF_OBJECTS_PROPERTY_NAME));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException ee) {
			ee.printStackTrace();
		}catch (Exception ex){
			ex.printStackTrace();
		}
	}

	public SequentialPartitioner() {
		super();

	}

	@Override
	public <E extends JessyEntity> Set<Group> resolve(ReadRequest<E> readRequest) {
		Set<Group> ret = new HashSet<Group>();

		if (readRequest.isOneKeyRequest()) {
			ret.add(resolve(readRequest.getOneKey().getKeyValue().toString()));
		} else {
			// TODO
			return null;
		}

		return ret;
	}

	@Override
	public boolean isLocal(String k) {
		return JessyGroupManager.getInstance().getMyGroups()
				.contains(resolve(k));
	}

	@Override
	public Set<String> resolveNames(Set<String> keys) {
		Set<String> result = new HashSet<String>();

		/*
		 * return if there is no key!
		 */
		if (keys.size() == 0)
			return result;

		for (String key : keys) {
			result.add(resolve(key).name());
		}
		logger.debug("keys " + keys + " are resolved to" + result);
		return result;
	}

	/**
	 * This methods returns the group of a key.
	 * 
	 * @param k
	 *            a key
	 * @return the replica group of <i>k</i>.
	 */
	private static Group resolve(String key) {
		int numericKey = 0;
		String mkey = key.replaceAll("[^\\d]", "");
		if (!mkey.equals("")) {
			numericKey = Integer.valueOf(mkey);
		}
		
		if (numberOfObjectsPerGroup==-1)
			numberOfObjectsPerGroup=totalNumberOfObjects/JessyGroupManager.getInstance().getReplicaGroups()
			.size();
		
		for(int i=0;i<JessyGroupManager.getInstance().getReplicaGroups()
				.size();i++){
			if (numericKey<=((i+1)*numberOfObjectsPerGroup)){
				return JessyGroupManager
						.getInstance()
						.getReplicaGroups()
						.get(i);
			}
		}
		
		return null;

	}

}
