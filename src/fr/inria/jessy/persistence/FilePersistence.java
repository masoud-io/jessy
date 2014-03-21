package fr.inria.jessy.persistence;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.protocol.ProtocolFactory;
import fr.inria.jessy.vector.GMUVector2;
import fr.inria.jessy.vector.ScalarVector;
import fr.inria.jessy.vector.VersionVector;

/**
 * This class stores and loads back any object given to it.
 * 
 * Data are stored and loaded from the following path:
 * [path]\number_of_groups\consistency\groupIndex_objectName
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class FilePersistence {

	public static boolean saveToDisk=false;
	public static boolean loadFromDisk=false;
	
	public static String storageDirectory="";

	public static void makeStorageDirectory(JessyGroupManager m, String path) {
		if (!path.endsWith("/"))
			path = path + "/";
		
		String group=""+m.getReplicaGroups().indexOf(m.getMyGroup());
		String numberOfgroups = "" + m.getReplicaGroups().size();
		//If the group size is 1, we add underscore to the path (thus the path is 1_), because the scripts needs to write to a file named "1".
		if (numberOfgroups.equals("1"))
			numberOfgroups=numberOfgroups + "_";
		String persistenceName = ProtocolFactory.getProtocolName();
		String result=path + numberOfgroups + "/" + persistenceName + "/" ;
		File file=new File(result);
		if (!file.exists())
			file.mkdirs();
		
		
		storageDirectory= result + group + "_";
		
		System.out.println("Storage Director is " + storageDirectory);
	}

	public static void writeObject(Object object, String objectName) {
		if (!saveToDisk)
			return;
		
		try {
			String filePath = storageDirectory + objectName;

			FileOutputStream fout = new FileOutputStream(filePath);
			ObjectOutputStream oos = new ObjectOutputStream(fout);
			oos.writeObject(object);
			oos.close();
			System.out.println("Wrote " + objectName + " successfully.");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static Object readObject(String objectName) {
		if (!loadFromDisk)
			return null;
			
		String filePath = storageDirectory + objectName;
		File file = new File(filePath);
		if (!file.exists()) {
			System.out.println("***ERROR***: File "+file+" does not exist.");
			return null;
		}
		
		try {
			FileInputStream fin = new FileInputStream(filePath);
			ObjectInputStream ois = new ObjectInputStream(fin);
			Object object = (Object) ois.readObject();
			ois.close();
			System.out.println("Read " + objectName + " successfully.");
			
			return object;

		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

}
