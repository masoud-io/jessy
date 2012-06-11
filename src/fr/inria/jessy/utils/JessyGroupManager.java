package fr.inria.jessy.utils;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import com.yahoo.ycsb.YCSBEntity;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.partitioner.Partitioner;
import fr.inria.jessy.partitioner.PartitionerFactory;

/**
 * This class wrap up the complexity of {@code FractalManager} by simplifying
 * and uniforming the access for different groups needed inside Jessy.
 * <p>
 * Note: any need for any Fractal group should be made through this class.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class JessyGroupManager {

	private static Logger logger = Logger.getLogger(JessyGroupManager.class);

	private static JessyGroupManager instance;

	private FractalManager fractal;

	private Partitioner partitioner;

	private int sourceId;

	/**
	 * Dependeing on whether this jessy instance is a proxy or a replica,
	 * myGroup is either myReplicaGroup or groupOfAllInstances
	 */
	private Group myGroup;

	/**
	 * The group of all running jessy instances in the system
	 */
	private Group groupOfAllInstances;

	/**
	 * The collection of different groups that this jessy instance is member of.
	 * This collection is used in the {@code Partitioner#isLocal(String)} in
	 * order to check whether a group of a key isLocal or not.
	 */
	private Collection<Group> myGroups;

	/**
	 * The sorted list of all replica groups. I.e., Only jessy instances that
	 * replicate jessy entities are in this list.
	 */
	private List<Group> replicaGroups;

	/**
	 * All groups created by fractal membership are in allGroups. This
	 * collection is only used for defining the acceptors needed to perform
	 * AtomcBroadcast in {@code NonGenuineTerminationCommunication}.
	 * 
	 */
	private Collection<Group> allGroups;

	/**
	 * Shows whether the Jessy instance is proxy or not. A jessy instance is
	 * proxy, if it is only being used for forwarding the transactions to the
	 * entity replicas
	 */
	private boolean isProxy;

	private JessyGroupManager() {

		try {

			String fractalFile = Configuration
					.readConfig(ConstantPool.FRACTAL_FILE);

			/*
			 * Initialize Fractal: create server groups, initialize this node
			 * and create the global group.
			 */
			{
				fractal = FractalManager.getInstance();
				fractal.loadFile(fractalFile);
				fractal.membership
						.dispatchPeers(ConstantPool.JESSY_SERVER_GROUP,
								ConstantPool.JESSY_SERVER_PORT,
								ConstantPool.GROUP_SIZE);

				fractal.membership.loadIdenitity(null);
			}

			/*
			 * Initialize allGroups. Is used as the acceptors for
			 * AtomicBroadcast
			 */
			allGroups = new TreeSet<Group>(fractal.membership.allGroups());

			myGroups = fractal.membership.myGroups();
			logger.debug("myGroups are: " + myGroups);

			Group myReplicaGroup = !fractal.membership.myGroups().isEmpty() ? fractal.membership
					.myGroups().iterator().next()
					: null;

			Group allReplicaGroup = fractal.membership.getOrCreateTCPGroup(
					ConstantPool.JESSY_ALL_REPLICA_GROUP,
					ConstantPool.JESSY_ALL_REPLICA_PORT);
			Collection<Integer> replicas = new HashSet<Integer>(
					fractal.membership.allNodes());

			if (myReplicaGroup == null)
				replicas.remove(fractal.membership.myId());
			allReplicaGroup.putNodes(replicas);

			groupOfAllInstances = fractal.membership
					.getOrCreateTCPDynamicGroup(ConstantPool.JESSY_ALL_GROUP,
							ConstantPool.JESSY_ALL_PORT);
			groupOfAllInstances.putNodes(fractal.membership.allNodes());

			logger.debug("groupOfAllInstances is : " + groupOfAllInstances);

			fractal.start();

			isProxy = myReplicaGroup == null;
			if (!isProxy) {
				logger.info("Server mode (" + myReplicaGroup + ")");
				myGroup = myReplicaGroup;
			} else {
				logger.info("Proxy mode");
				myGroup = groupOfAllInstances;
			}

			logger.debug("myGroup is : " + myGroup);

			replicaGroups = new ArrayList<Group>(
					fractal.membership
							.allGroups(ConstantPool.JESSY_SERVER_GROUP));
			Collections.sort(replicaGroups);

			logger.debug("replicaGroups are : " + replicaGroups);

			sourceId = fractal.membership.myId();

			// TODO generalize this
			partitioner = PartitionerFactory
					.getPartitioner(YCSBEntity.keyspace);

		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}
	}

	public static JessyGroupManager getInstance() {
		if (instance == null)
			instance = new JessyGroupManager();

		return instance;

	}

	public Group getMyGroup() {
		return myGroup;
	}

	public Collection<Group> getMyGroups() {
		return myGroups;
	}

	public int getSourceId() {
		return sourceId;
	}

	public List<Group> getReplicaGroups() {
		return replicaGroups;
	}

	public Collection<Group> getAllGroups() {
		return allGroups;
	}

	public Partitioner getPartitioner() {
		return partitioner;
	}

	public Group getGroupOfAllInstances() {
		return groupOfAllInstances;
	}

	public boolean isProxy() {
		return isProxy;
	}
}
