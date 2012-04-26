package com.yahoo.ycsb;


import java.util.HashMap;
import java.util.Set;

import com.sleepycat.persist.model.Entity;
import com.yahoo.ycsb.workloads.CoreWorkload;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.Keyspace;
import fr.inria.jessy.store.Keyspace.Distribution;

@Entity
public class YCSBEntity extends JessyEntity{
	
	private static final long serialVersionUID = 1L;
	
	public static Keyspace keyspace = 
		new Keyspace(CoreWorkload.TABLENAME_PROPERTY_DEFAULT+":user########",Distribution.UNIFORM);
	
	public YCSBEntity(){
		super("","");
	}

	
	/*Fields of the entity*/
	private HashMap<String, String> fields=new HashMap<String, String>();
	public void setFields(HashMap<String, String> fields) {
		this.fields = fields;
	}

	/*Constructor*/
	public YCSBEntity(String entityClassName, String entityId) {
		super(entityClassName, entityId);
	}

	/*Constructor Using a hashmap already Definded */
	public YCSBEntity (String entityClassName, String entityId, HashMap <String,String> insFields) {

		this(entityClassName,entityId);

		for (String k : insFields.keySet()) {
			this.fields.put(k, insFields.get(k));
		}
	}
	/*Get the Set of fields' names (Keys)*/
	public Set<String> getFields() {
		return fields.keySet();
	}
	/*Add a new field*/
	public void put(String field, String value) {
		if (fields == null) fields = new HashMap<String, String>();
			fields.put(field, value);
	}
	/*Get a field*/
	public String getFieldValue(String fieldName) {
		return fields.get(fieldName);
	}

	/*Delete field*/
	public void remove (String key) {
		fields.remove(key);
	}
	/*Clear all fields */
	

	public void removeAll() {
		fields.clear();
		fields = null;

	}



}
