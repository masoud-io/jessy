package com.yahoo.ycsb;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Set;

import com.sleepycat.persist.model.Entity;
import com.yahoo.ycsb.workloads.CoreWorkload;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.Keyspace;
import fr.inria.jessy.store.Keyspace.Distribution;
import fr.inria.jessy.utils.Compress;

@Entity
public class YCSBEntity extends JessyEntity implements Externalizable {

	private static final long serialVersionUID = 1L;

	public static Keyspace keyspace = new Keyspace(
			"user########",
			Distribution.UNIFORM);

	public YCSBEntity() {
		super("", "");
	}

	/* Fields of the entity */
	private HashMap<String, String> fields;

	public void setFields(HashMap<String, String> fields) {
		this.fields = fields;
	}

	/* Constructor */
	public YCSBEntity(String entityClassName, String entityId) {
		super(Compress.compressClassName(entityClassName), entityId);
	}

	/* Constructor Using a hashmap already Definded */
	public YCSBEntity(String entityClassName, String entityId,
			HashMap<String, String> insFields) {

		this(entityClassName, entityId);

		this.fields = insFields;
	}

	/* Get the Set of fields' names (Keys) */
	public Set<String> getFields() {
		return fields.keySet();
	}

	/* Add a new field */
	public void put(String field, String value) {
		if (fields == null)
			fields = new HashMap<String, String>();
		fields.put(field, value);
	}

	/* Get a field */
	public String getFieldValue(String fieldName) {
		return fields.get(fieldName);
	}

	/* Delete field */
	public void remove(String key) {
		fields.remove(key);
	}

	/* Clear all fields */

	public void removeAll() {
		fields.clear();
		fields = null;

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(fields);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		fields = (HashMap<String, String>) in.readObject();
	}

}
