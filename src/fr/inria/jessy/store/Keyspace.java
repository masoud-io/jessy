package fr.inria.jessy.store;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class Keyspace {

	public static Keyspace DEFAULT_KEYSPACE= new Keyspace("################", Distribution.UNIFORM);
	
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
		if(!isCorrectDefinition(def)) throw new IllegalArgumentException();
		definition = def;
		distribution = dist;
	}
	
	private boolean isCorrectDefinition(String def){
		if(def.length()>16) return false;
		return true;
	}
	
	public String getDefinition(){
		return definition;
	}
	
	public Distribution getDistribution(){
		return distribution;
	}
	
	
	
}
