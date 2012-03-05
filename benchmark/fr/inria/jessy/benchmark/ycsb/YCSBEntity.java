package fr.inria.jessy.benchmark.ycsb;

import java.util.HashMap;
import java.util.Set;

import com.sleepycat.persist.model.Entity;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.SampleEntityClass;

@Entity
public class YCSBEntity extends JessyEntity {

	private HashMap<String, String> fields;

	public YCSBEntity(String entityClassName, String entityId) {
		super(entityClassName, entityId);

		fields = new HashMap<String, String>();

	}

	public Set<String> getFields() {
		return fields.keySet();
	}

	public void addField(String f) {
		getFields().add(f);
	}

	public void addFieldID(String fieldID) {
		fields.keySet().add(fieldID);
	}

	public void put(String field, String value) {
		fields.put(field, value);
	}

	public void getField(String field) {
		fields.get(field);
	}

	@Override
	public String getKey() {
		return SampleEntityClass.class.toString() + this.getSecondaryKey();
	}

}
