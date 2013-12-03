package fr.inria.jessy.partitioner;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import net.sourceforge.fractal.membership.Group;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.Keyspace;
import fr.inria.jessy.store.Keyspace.Distribution;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.store.ReadRequestKey;

/**
 * This class implements a partitioner, that is a function that maps objects
 * to groups.
 * 
 * The assign method of the partitioner takes as input a keyspace and a
 * distribution and computes a set of rootkeys inside the keyspace.
 * 
 * For some key k, the replica group of k is the group holding the rootkey rk
 * having the smallest Levenshtein distance to k.
 * 
 * 
 */

public class KeySpacePartitioner  extends Partitioner{
	
	private static Logger logger = Logger.getLogger(KeySpacePartitioner.class);
	
	private HashSet<Keyspace> keyspaces; // TODO check intersection
	private TreeMap<Group, Set<String>> g2rk; // groups to rootkeys
	private TreeMap<String, Group> rk2g; // rootkeys to groups

	public KeySpacePartitioner(JessyGroupManager m, Keyspace keyspace) {
		super(m);
		g2rk = new TreeMap<Group, Set<String>>();
		for (Group g : manager.getReplicaGroups()){
			g2rk.put(g, new HashSet<String>());
		}
		rk2g = new TreeMap<String, Group>();
		keyspaces = new HashSet<Keyspace>();
		
		assign(keyspace);
	}

	/**
	 * Assign rootkeys to a group according to the keyspace definition <i>ks</i>
	 * and the distribution <i>dist</i> such that each group maintains the same
	 * amount of keys to be requested according to the probalistic distribution.
	 * 
	 * The keyspace definition is a string whose length equals 32 characters, 
	 * and that matches the following regex: (([^\#]*)\#*)*.
	 * 
	 * It specifies the set of keys in the keyspace.
	 * The keyspace is exactly the set of strings that 
	 * matches the regex obtained by transforming in the keyspace definition 
	 * every character c except '#' as \c, and every character '#' is .
	 * the class [0-9]
	 * 
	 * For instance, the keyspace: bonjour#iciTerre!
	 * is the set of strings matching the regex: bonjour[0-9]iciTerre\! .
	 *   
	 * @param ks
	 *            a keyspace definition
	 *            If ks is not of size 32, a left padding with '\0' occurs.
	 *            If ks is empty, then the keyspace equals '#################'. 
	 * @param a
	 *            distribution
	 */
	public <E extends JessyEntity> void assign(Keyspace keyspace)
			throws IllegalArgumentException {

		if(keyspace==null)
			throw new IllegalArgumentException("null keyspace");
		
		if(keyspaces.contains(keyspace))
			return;
		
		
		// Check that keyspace is either 
		// (i) disjoint, or (ii) a proper partitioning of 
		// already existing keyspaces.
		// 
		// TODO
		keyspaces.add(keyspace);
		
		Distribution dist = keyspace.getDistribution();
		String definition = keyspace.getDefinition();
		
		if (dist == Keyspace.Distribution.UNIFORM) {
				
			String rootkey;
			String rs;
			Group g;
			Double range;
			int pos,
				ngroups=g2rk.size(),
				nsharps=StringUtils.countMatches(definition, "#");
			
			for (int i = 0; i < ngroups && (Math.pow(10, nsharps)>i); i++) {
			
				g = (Group) g2rk.keySet().toArray()[i];
				double a = (double)i/(double)ngroups;
				double b = Math.pow(10,nsharps);
				range = new Double(Math.floor(a*b));
				rs = String.format(
						"%"+(nsharps==0 ? "" : "0"+nsharps)+"d",
						range.longValue());
				rootkey = new String();
				pos=0;
				
				for(int j=0; j<definition.length(); j++){
					if( definition.charAt(j) == '#' ){
						rootkey+=rs.charAt(pos);
						pos++;
					}else{
						rootkey+=definition.charAt(j);
					}
				}
				
				assert g2rk.containsKey(g);
				g2rk.get(g).add(rootkey);
				rk2g.put(rootkey,g);
				logger.debug("assigning rootkey "+rootkey+" to"+g);
			}
			
		} else {
			throw new RuntimeException("NIY");
		}
		
		

	}

	@Override
	public <E extends JessyEntity> Set<Group> resolve(ReadRequest<E> readRequest) {		
		
		Set<Group> ret = new HashSet<Group>();
		
		if( readRequest.isOneKeyRequest() ){
			ret.add(rk2g.get(closestRootkeyOf(readRequest.getOneKey().getKeyValue().toString()))); // FIXME !!! what is this parametric type ....
		}else{
			//FIXME Incorrect implementation
			for(ReadRequestKey key : readRequest.getMultiKeys())
				ret.add(rk2g.get(closestRootkeyOf(key.getKeyValue().toString())));
		}
		return ret;
	}

	@Override
	public Set<String> resolveNames(Set<String> keys) {
		Set<String> results = new HashSet<String>();
		for (String key : keys) {
			results.add(rk2g.get(closestRootkeyOf(key)).name());
		}
		logger.debug("keys " + keys + " are resolved to" + results);
		return results;
	}

	@Override
	public boolean isLocal(String k) {
		boolean ret = manager.getMyGroups().contains(resolve(k));
//		logger.debug("is local "+k+" ? "+ret);
		return ret;
	}
	
	//
	// INNER METHODS
	//

	/**
	 * This methods returns the group of a key.
	 * 
	 * @param k a key
	 * @return the replica group of <i>k</i>.
	 */
	public Group resolve(String k) {
		String closest = closestRootkeyOf(k);
		Group ret = rk2g.get(closest);
		assert ret!=null;
		return ret;
	}
	
	/**
	 * @param k a key 
	 * @return the closest rootkey.
	 */
	private String closestRootkeyOf(String k) {
		String closest=null;
		BigInteger dr,dc=null;
		for (String r : rk2g.keySet()) {
			dr = distance(r,k);
			if(dc==null
			   ||
			   dr.compareTo(dc)<0){
				closest=r;
				dc=dr;
			}
		}
		assert closest != null;
//		logger.debug("closest key to "+k+" is "+closest+" ("+rk2g.get(closest)+")");
		return closest;
	}

	private BigInteger distance(String k1, String k2) {
		BigInteger v1 = new BigInteger(k1.getBytes());
		BigInteger v2 = new BigInteger(k2.getBytes());
		return v1.subtract(v2).abs();		
	}

	@Override	
	public Set<String> generateKeysInAllGroups() {
		//TODO
		return null;
	}
	
	@Override
	public Set<String> generateLocalKey() {
		//TODO
		return null;
	}
}
