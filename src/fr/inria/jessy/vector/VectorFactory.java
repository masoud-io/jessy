package fr.inria.jessy.vector;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.protocol.ProtocolFactory;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

public class VectorFactory {

	private static String protocolName = ProtocolFactory.getProtocolName();
	
	private JessyGroupManager manager;
	
	public static <K> Vector<K> GetVector(K selfKey) {

//		if (protocolName.contains("Walter")) {
//			return new VersionVector(manager.getMyGroup().name(), 0);
//		}
		if (protocolName.contains("_dv_")) {
			return new DependenceVector<K>(selfKey);
		}
		if (protocolName.contains("_pdv_")) {
			return new PartitionDependenceVector<K>(selfKey,0);
		}
		if (protocolName.contains("rc")) {
			return new NullVector<K>(selfKey);
		}
		if (protocolName.contains("_vv_")) {
			return new VersionVector<K>(selfKey, 0);
		}
		if (protocolName.contains("_sv_")) {
			return new ScalarVector<K>();
		}
		if (protocolName.contains("gmv") ) {
			return new GMUVector<K>();
		}
		if (protocolName.contains("lsv") ) {
			return new LightScalarVector<K>(selfKey);
		}
		return null;
	}
	
	public VectorFactory(JessyGroupManager m) {
		manager = m;
		if (protocolName.equals("walter")) {
			VersionVector.init(manager);
		}
		else if (protocolName.equals("nmsi_gmv2_gc") || protocolName.equals("us_gmv2_gc")) {
			GMUVector2.init(manager);
		}	
		else if (protocolName.equals("nmsi_gmv_gc") || protocolName.equals("us_gmv_gc")
				|| protocolName.equals("gmu")) {
			GMUVector.init(manager);
		}	
	}	
	
	public static boolean prepareRead(ReadRequest rr){
		if (protocolName.equals("nmsi_gmv_gc") || protocolName.equals("us_gmv_gc")
				|| protocolName.equals("gmu")) {
			return GMUVector.prepareRead(rr);
		}
		else if (protocolName.equals("nmsi_gmv2_gc") || protocolName.equals("us_gmv2_gc")) {
			return GMUVector2.prepareRead(rr);
		}
		else
			return true;
	}
	
	public static void postRead(ReadRequest rr, JessyEntity entity){
		if (protocolName.equals("nmsi_gmv_gc") || protocolName.equals("us_gmv_gc")
				|| protocolName.equals("gmu")) {
			GMUVector.postRead(rr, entity);
		}
		if (protocolName.equals("nmsi_gmv2_gc") || protocolName.equals("us_gmv2_gc")) {
			GMUVector2.postRead(rr, entity);
		}
	}

	/**
	 * This method is not NECESSARY.
	 * It is just a dirty way to improve performance.
	 *  
	 * @return true if {@link Vector#updateExtraObjectInCompactVector(Vector, Object)} is implemented in the 
	 * corresponding vector. 
	 * 
	 */
	public boolean needExtraObject() {

		if (protocolName.equals("nmsi_gmv_gc") || 
				protocolName.equals("us_gmv_gc") ||
				protocolName.equals("gmu") || 
				protocolName.equals("nmsi_gmv2_gc") ||
				protocolName.equals("us_gmv2_gc") || 
				protocolName.equals("nmsi_pdv_gc")) {
			return true;
		}

		return false;
	}

}
