package fr.inria.jessy;

import java.io.File;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;

import fr.inria.jessy.store.DataStore;

/**
 * @author Masoud Saeida Ardekani
 * 
 */
public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		
		String executionPath = System.getProperty("user.dir");
		System.out.println(executionPath);

		try {
			DataStore ds = new DataStore(new File(executionPath), false,
					"myStore");
			
			ds.addPrimaryIndex("myStore", EntClass.class);
			
			ds.addPrimaryIndex("myStore", EntClass.class);

			ds.addSecondaryIndex("myStore", EntClass.class, String.class,
					"classValue");

			EntClass ec = new EntClass();
			ec.setClassValue("OK");
			ec.setData("ver1");
			ds.put(ec);

			ec = new EntClass();
			ec.setClassValue("OK");
			ec.setData("ver2");
			ds.put(ec);

			EntClass ecc = ds.get( EntClass.class, "classValue",
					"OK", null);

			System.out.println("put done: " + ecc.getData().toString());
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		// EntityStore store=ds.getEntityStores().get("myStore");
		//
		// AccessEntClass access=new AccessEntClass(store);
		// AccessEntClass2 access2=new AccessEntClass2(store);

		// EntClass ec=new EntClass("s1", "Value");
		// access.pindex.put(ec);
		// ec=new EntClass("s1", "Value");
		// access.pindex.put(ec);
		//
		// EntityCursor<EntClass> cur= access.sindex.subIndex("s1").entities();
		// for(EntClass ecc: cur){
		// System.out.println(ecc.getSecondaryKey().toString());
		// }

		// EntClass2 ec2=new EntClass2("p1", "s1", "Value1");
		// access2.pindex.put(ec2);
		//
		// ec2=new EntClass2("p1", "s2", "Value2");
		// access2.pindex.put(ec2);
		//
		// EntityCursor<EntClass2> cur=access2.pindex.entities();
		// for(EntClass2 ecc: cur){
		// System.out.println(ecc.getClassValue().toString());
		// }

	}

}
