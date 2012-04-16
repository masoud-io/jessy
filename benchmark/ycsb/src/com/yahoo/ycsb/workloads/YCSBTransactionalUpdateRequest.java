package com.yahoo.ycsb.workloads;

import java.util.HashMap;

public class YCSBTransactionalUpdateRequest {

	public String table;
	public 	String key;
	public HashMap<String, String> values;

	public YCSBTransactionalUpdateRequest(String table, String key,
			HashMap<String, String> values) {
		this.table = table;
		this.key = key;
		this.values = values;
	}
}
