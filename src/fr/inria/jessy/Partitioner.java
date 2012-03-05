package fr.inria.jessy;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;

import org.apache.commons.lang3.StringUtils;

/**
 * This class implements a partitioner, that is a function that maps
 * object keys to groups.
 * 
 * The assign method of the partitioner takes as input a keyspace and a distribution
 * and computes a set of rootkeys inside the keyspace.
 *   
 * For some key k, the replica group of k is the group holding the rootkey rk
 * having the smallest Levenshtein distance to k. 
 * 
 * A keyspace definition matches the following regex: ([:xdigit:]*#)*
 * For instance, warhouse#:district# is a valid keyspace.
 * It is interpreted as the set of strings that matches the regex warhouse[:digits:]district[:digits:].
 * That is the character # is a shortcut for the class [:digits:].
 * 
 * The distribution can be of three types: uniform, normal or zipf.
 * The rootkeys are attributed using both the keyspace and the distribution
 * such that two rootkeys are close according to the distribution to the same set of keys. 
 * For instance, if the keyspace is warhouse# ,the distribution is uniform,
 * and there are 3 groups g1, g2 and g3, the rootkeys of g1 and g2 and g3 are respectively
 * warhouse0, warhouse3 and warhouse6.
 *   
 * @author P.Sutra
 *
 */

/*
 * TODO Use locality preserving hash functions and a fixed key space, e.g., 2^16 bits ? 
 * 		=> code of Cassandra ? 
 */

public class Partitioner {
	
	private static Partitioner instance;
	
	public static enum Distribution{
		UNIFORM,
		NORMAL,
		ZIPF
	};
	
	private Map<Group,Set<String>> g2rk; // groups to rootkeys
	private Map<String,Group> rk2g; // rootkeys to groups

	public static Partitioner getInstance() {
		if(instance==null) instance = new Partitioner();
		return instance;
	}
	
	private Partitioner(){
		g2rk = new HashMap<Group, Set<String>>();
		for(Group g : Membership.getInstance().allGroups()){
			g2rk.put(g,new HashSet<String>());
		}
		rk2g = new HashMap<String, Group>();
	}
	
	/**
	 * Assign rootkeys to a group according to the keyspace definition <i>ks</i> 
	 * and the distribution <i>dist</i> such that each group maintains the same 
	 * amount of keys to be requested according to the probalistic distribution.
	 * 
	 * @param rk a keyspace definition
	 * @param a distribution
	 */
	// TODO: do not take all ports!
	public void assign(String ks, Distribution dist) throws InvalidParameterException{
		
		Pattern p = Pattern.compile("([:xdigit:]*#)*");
		Matcher m = p.matcher(ks);
		if(!m.find()) throw new InvalidParameterException("Incorrect keyspace definition");
		 
		if(dist==Distribution.UNIFORM){
			String [] ms = ks.split("#");
			String rootkey;
			for(int i=0; i<Membership.getInstance().allGroups().size();i++){
				rootkey = new String();
				for(char c : Integer.toString(i*(10^(ms.length)/Membership.getInstance().allGroups().size())).toCharArray()){
					rootkey+=ms[i]+c;
				}
				g2rk.get(Membership.getInstance().allGroups().toArray()[i]).add(rootkey);
				rk2g.put(rootkey, (Group)Membership.getInstance().allGroups().toArray()[i]);
			}
		}else{
			throw new RuntimeException("NIY");
		}
			
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
	
	public boolean isLocal(String k){
		return Membership.getInstance().myGroups().contains(resolve(k));
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
