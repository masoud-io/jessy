package com.yahoo.ycsb;
public class Operation {
	private OPState OPState;
	private int OPOrder;
	private String OPKey;
	private OPType OPType;
	
	public Operation(int n,String key,OPState state,OPType type) {
		this.OPKey = key;
		this.OPOrder = n;
		this.OPState = state;
		this.OPType = type;
	}
	
	public String toString() {
		return OPOrder+"\t"+OPType+"\t"+OPKey+"\t"+OPState+"\n";
	}
	public void setState(OPState st) {
		this.OPState = st;
	}
}