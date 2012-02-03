package fr.inria.jessy;




import java.io.File;

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
		
		DataStore ds=new DataStore(new File(executionPath), false, "myStore");
		
		EntityStore store=ds.getEntityStores().get("myStore");
		
		AccessEntClass access=new AccessEntClass(store);
		AccessEntClass2 access2=new AccessEntClass2(store);
		
		EntClass ec=new EntClass("p1", "s1", "Value");
		access.pindex.put(ec);
		
		EntClass2 ec2=new EntClass2("p1", "s2", "Value2");
		access2.pindex.put(ec2);

		EntClass readc=access.pindex.get("p1");
		EntClass2 readc2=access2.pindex.get("p1");
		System.out.println(readc.getsKey().toString());
		System.out.println(readc2.getsKey().toString());
	}

}
