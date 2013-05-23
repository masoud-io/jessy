package fr.inria.jessy.vector;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.consistency.ProtocolFactory;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

public class VectorFactory {

	private static String persistenceName = ProtocolFactory.getPersistenceName();
	
	private JessyGroupManager manager;
	
	public VectorFactory(JessyGroupManager m) {
		manager = m;
		if (persistenceName.equals("psi")) {
			VersionVector.init(manager);
		}
		if (persistenceName.equals("nmsi3") || persistenceName.equals("us3")) {
			GMUVector2.init(manager);
		}	
		if (persistenceName.equals("us2")) {
			GMUVector.init(manager);
		}	
	}	
	
	public static boolean prepareRead(ReadRequest rr){
		if ( persistenceName.equals("us2")) {
			return GMUVector.prepareRead(rr);
		}
		else if (persistenceName.equals("nmsi3") || persistenceName.equals("us3")) {
			return GMUVector2.prepareRead(rr);
		}
		else
			return true;
	}
	
	public static void postRead(ReadRequest rr, JessyEntity entity){
		if ( persistenceName.equals("us2")) {
			GMUVector.postRead(rr, entity);
		}
		if (persistenceName.equals("nmsi3") || persistenceName.equals("us3")) {
			GMUVector2.postRead(rr, entity);
		}
	}

	public boolean needExtraObject() {

		if (persistenceName.equals("nmsi2") || persistenceName.equals("us2")) {
			return true;
		}

		return false;
	}

}
