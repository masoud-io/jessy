package fr.inria.jessy.persistence;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.consistency.ConsistencyFactory;
import fr.inria.jessy.vector.GMUVector;
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
		String consistency = ConsistencyFactory.getConsistencyTypeName();
		String result=path + numberOfgroups + "/" + consistency + "/" ;
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
			System.out.println("File "+file+" does not exist.");
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

	/**
	 * We only need to store commitedVTS of VersionVector and sequenceNumber of ScalarVector
	 */
	public static void saveJessy() {
		System.out.println("Saving Jessy ...");
		if (ConsistencyFactory.getConsistencyTypeName().equals("psi"))
			writeObject(VersionVector.committedVTS, "VersionVector.committedVTS");
		else if (ConsistencyFactory.getConsistencyTypeName().equals("si2") || ConsistencyFactory.getConsistencyTypeName().equals("si"))
			writeObject(ScalarVector.lastCommittedTransactionSeqNumber, "ScalarVector.lastCommittedTransactionSeqNumber");
		else if (ConsistencyFactory.getConsistencyTypeName().equals("nmsi2") || ConsistencyFactory.getConsistencyTypeName().equals("us2")){
			writeObject(GMUVector.lastPrepSC, "GMUVector.lastPrepSC");
			writeObject(GMUVector.mostRecentVC, "GMUVector.mostRecentVC");			
		}
		
	}
}
