package fr.inria.jessy.store;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.sleepycat.persist.model.Persistent;

/**
 * A keyspace definition matches the following regex: ([:xdigit:]*#)* 
 * For instance, warhouse#:district# is a valid keyspace. It is interpreted as the
 * set of strings that matches the regex warhouse[:digits:]district[:digits:].
 * That is the character # is a shortcut for the class [:digits:].
 * 
 * The distribution can be of three types: uniform, normal or zipf. The rootkeys
 * are attributed using both the keyspace and the distribution such that two
 * rootkeys are close according to the distribution to the same number of keys. For
 * instance, if the keyspace is warhouse# ,the distribution is uniform, and
 * there are 3 groups g1, g2 and g3, the rootkeys of g1 and g2 and g3 are
 * respectively warhouse0, warhouse3 and warhouse6.
 * @author otrack
 *
 */

@Persistent
public class Keyspace {

	public static Keyspace DEFAULT_KEYSPACE= 
		new Keyspace(
				"00000000000000000000000000000000" 
				, Distribution.UNIFORM);
	
	public static enum Distribution {
		UNIFORM, 
		NORMAL, 
		ZIPF
	}
	
	private String definition;
	private Distribution distribution;
	
	//Default constructor is needed for BerkeleyDB
	public Keyspace(){
		
	}
	
	public Keyspace(String def, Distribution dist) throws IllegalArgumentException {

		// Check correctness of the input keyspace
		if(!isCorrectDefinition(def))
			throw new IllegalArgumentException("Invalid keyspace definition");
		
		// Left-padding if necessary:
		StringUtils.leftPad(def,32,"\0");

		definition = def;
		distribution = dist;
	}
	
	private boolean isCorrectDefinition(String def){ 
		if(def.length()>32)
			return false;
		Pattern ps = Pattern.compile("^(([^#])*#*)+$");
		Matcher m = ps.matcher(def);
		if (!m.find())
			return false;
		return true;
	}
	
	public String getDefinition(){
		return definition;
	}
	
	public Distribution getDistribution(){
		return distribution;
	}
	
}
