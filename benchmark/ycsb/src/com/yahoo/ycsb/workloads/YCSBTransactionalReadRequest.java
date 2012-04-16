package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.Set;

public class YCSBTransactionalReadRequest {

	public String table;
	public String key;
	public Set<String> fields;
	public HashMap<String, String> result;

	public YCSBTransactionalReadRequest(String table, String key,
			Set<String> fields, HashMap<String, String> result) {
		this.table = table;
		this.key = key;
		this.fields = fields;
		this.result = result;
	}

}
