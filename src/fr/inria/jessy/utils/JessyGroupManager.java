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

public class JessyGroupManager {

	private static Logger logger = Logger.getLogger(JessyGroupManager.class);

	private static JessyGroupManager instance;

	private int sourceId;

	private Partitioner partitioner;

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
	 */
	private Collection<Group> myGroups;

	/**
	 * The sorted list of all replica groups. I.e., Only jessy instances that
	 * replicate jessy entities are in this list.
	 */
	private List<Group> replicaGroups;

	/**
	 * All groups created by fractal membership are in allGroups.
	 */
	private Collection<Group> allGroups;

	private boolean isProxy;

	private FractalManager fractal;

	private JessyGroupManager() {

		try {

			String fractalFile = Configuration
					.readConfig(ConstantPool.FRACTAL_FILE);

			/*
			 * Initialize Fractal: create server groups, initialize this node
			 * and create the global group.
			 */
			fractal = FractalManager.getInstance();
			fractal.loadFile(fractalFile);

			fractal.membership.dispatchPeers(ConstantPool.JESSY_SERVER_GROUP,
					ConstantPool.JESSY_SERVER_PORT, ConstantPool.GROUP_SIZE);

			fractal.membership.loadIdenitity(null);

			allGroups = new TreeSet<Group>(fractal.membership.allGroups());

			myGroups = fractal.membership.myGroups();

			logger.debug("myGroups are: " + myGroups);

			Group myReplicaGroup = !fractal.membership.myGroups().isEmpty() ? fractal.membership
					.myGroups().iterator().next()
					: null; // this node is a
							// server ?

			Group allReplicaGroup = fractal.membership.getOrCreateTCPGroup(
					ConstantPool.JESSY_ALL_REPLICA_GROUP,
					ConstantPool.JESSY_ALL_REPLICA_PORT);
			Collection<Integer> replicas = new HashSet<Integer>(
					fractal.membership.allNodes());

			if (myReplicaGroup == null)
				replicas.remove(fractal.membership.myId());
			allReplicaGroup.putNodes(replicas);

			Group groupOfAllInstances = fractal.membership
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
		} catch (Exception ex) {
			ex.printStackTrace();
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
