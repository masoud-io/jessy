package fr.inria.jessy.benchmark.ycsb;


import java.util.HashMap;
import java.util.Set;

import com.sleepycat.persist.model.Entity;

import fr.inria.jessy.store.JessyEntity;

@Entity
public class YCSBEntity extends JessyEntity{
	
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

	/* Get the key of the Entity */
	public String getKey() {
		return YCSBEntity.class.toString() + this.getSecondaryKey();
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
