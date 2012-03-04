package com.yahoo.ycsb.jessy;

import fr.inria.jessy.store.JessyEntity;

public class Test {
	
	
	
	public static void main (String[] args) {
		JessyEntity en = new JessyYCSBEntity();
		System.out.println(en.getClass().getName());
	}
}