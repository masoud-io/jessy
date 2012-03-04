package com.yahoo.ycsb.jessy;

import java.util.HashMap;
import java.util.Set;

import com.sleepycat.persist.model.Entity;

import fr.inria.jessy.store.JessyEntity;



@Entity
public class JessyYCSBEntity extends JessyEntity {
	private int order = 0;

	private HashMap <String , String > fields;
	public JessyYCSBEntity() {
		super("","");
	
		fields = new HashMap<String, String>();
		order++;
	}
	
	@Override
	public <T> String getLocalVectorSelfKey(T entityID) {
		
		return null;
	}
	
	
	public Set<String> getFields() {
		return fields.keySet();	
	}
	public void addField(String f){
		getFields().add(f);
	}
	
	public void addFieldID(String fieldID) {
		fields.keySet().add(fieldID);
	}
	
	public void put (String field,String value) {
		fields.put(field, value);
	}
	
	
	public void get ( String field ) {
		fields.get(field);
	}
	
}
