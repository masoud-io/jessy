package fr.inria.jessy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.sourceforge.fractal.membership.Group;
import org.apache.commons.lang3.StringUtils;


/**
 * This class implements a partitioner, that is a function that maps
 * keys to group of replica that will replicate it.
 * 
 * The partitioner works as follows: it takes as input a keyspace defined
 * as a couple (set of keys, distribution). It computes a set of rootkeys inside 
 * the keyspace, one for each group.
 * For some key k, the replica group of k is the group holding the rootkey rk
 * having the smallest Levenshtein's distance to k. 
 * 
 * The distribution can be of three types: uniform, normal or zipf.
 * A keyspace is of the form: 
 *     a+ where "a" is either a character or a class (in the regex sense) of characters.
 * 
 * 
 * @author P.Sutra
 *
 */

/*
 * 
 * TODO The distance used is Levenshtein's. 
 *      This is expensive: O(m^2) where m is the size of the strings.
 *      Besides, it seems not very appropriate for hierarchical structures
 *      where one can have things like:
 *      table1:row2 closer to table2:row1 than to table1:row2:att1
 *      
 * TODO Use locality preserving hash functions and a fixed key space, e.g., 2^16 bits ? 
 * 		=> code of Cassandra ? 
 * 
 */

public class Partitioner {
	
	public static enum Distribution{
		UNIFORM,
		NORMAL,
		ZIPF
	};
	
	private Set<Group> groups;
	private Map<Group,Set<String>> g2rk; // groups to rootkeys
	private Map<String,Group> rk2g; // rootkeys to groups
	
	public Partitioner(Set<Group> s){
		groups = s;
		g2rk = new HashMap<Group, Set<String>>();
		for(Group g : groups){
			g2rk.put(g,new HashSet<String>());
		}
		rk2g = new HashMap<String, Group>();
	}
	
	/**
	 * Assign a rootkey to a group.
	 * 
	 *  TODO define regex for a keyspace.
	 * 
	 * @param rk a root key
	 * @param dist the distribution 
	 */
	public void assign(String rk, Group g) {
		g2rk.get(g).add(rk);
		rk2g.put(rk, g);
	};
	
	/**
	 * This methods returns the group of a key.
	 * 
	 * @param k a key 
	 * @return replica group of <i>k</i>.
	 */
	public Group resolve(String k){
		return rk2g.get(closestRootkeyOf(k));
	}
	
	//
	// INNER METHODS 
	//
	
	/**
	 * @param k a key
	 * ^return the closest rootkey. 
	 */
	private String closestRootkeyOf(String k){
		String closest="";
		for(String r : rk2g.keySet()){
			if(distance(r,closest) < distance(r,k))
				closest=r;
		}
		return closest;
	}
	
	/** 
	 * @param k1 a key
	 * @param k2 a key 
	 * @return the Levenshtein distance between <i>k1</i> and <i>k2</i>.
	 */
	private int distance(String k1, String k2){
		return StringUtils.getLevenshteinDistance(k1, k2);
	}
	
}
